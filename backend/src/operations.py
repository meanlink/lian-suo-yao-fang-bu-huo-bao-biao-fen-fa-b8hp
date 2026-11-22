import json
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, HTTPException, Query
from mysql.connector import IntegrityError
from pydantic import BaseModel, ConfigDict, Field

from .auth import current_user, require
from .db import conn, query

router = APIRouter()
MANAGER = require('regional_manager')
INVENTORY = require('inventory_clerk', 'regional_manager')
SHIFT = require('shift_coordinator', 'regional_manager')
LOCAL_ZONE = ZoneInfo('Asia/Shanghai')
LATEST = '''NOT EXISTS (SELECT 1 FROM inventory_snapshots newer WHERE newer.store_code=s.store_code
            AND newer.sku=s.sku AND newer.snapshot_date>s.snapshot_date)'''
RECEIPT_GAP = '''d.status='sent' AND (d.dispatched_at IS NULL OR d.dispatched_at <= NOW() - INTERVAL 2 HOUR)'''
SHIFT_NAMES = ('morning', 'afternoon', 'night')


class Note(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)
    note: str = Field(min_length=3, max_length=255)


class ExpiryAction(Note):
    action: Literal['quarantined', 'returned']


class Subscription(BaseModel):
    store_code: str = Field(min_length=1, max_length=40)
    recipient_id: int = Field(gt=0)
    delivery_hour: int = Field(ge=0, le=23)


class Enabled(BaseModel):
    enabled: bool


def initialize():
    schema = Path(__file__).resolve().parents[1] / 'operations.sql'
    for statement in schema.read_text().split(';'):
        if statement.strip():
            query(statement, fetch=False)
    # 老订阅没有生效历史时补一条基线：以当前状态作为创建次日起生效的配置。
    query('''INSERT INTO report_schedule_changes(subscription_id,effective_from,enabled)
             SELECT rp.id,DATE_ADD(rp.created_on,INTERVAL 1 DAY),rp.enabled FROM report_subscriptions rp
             WHERE NOT EXISTS (SELECT 1 FROM report_schedule_changes c WHERE c.subscription_id=rp.id)''',
          fetch=False)


@contextmanager
def operational_transaction():
    db = conn()
    cur = db.cursor(dictionary=True, buffered=True)
    try:
        yield cur
        db.commit()
    except BaseException:
        db.rollback()
        raise
    finally:
        cur.close()
        db.close()


def audit(cur, actor, event, kind, entity_id, **detail):
    cur.execute('''INSERT INTO audit_logs(actor_id,action,entity_type,entity_id,detail) VALUES(%s,%s,%s,%s,%s)''',
                (actor, event, kind, entity_id, json.dumps(detail, ensure_ascii=False)))


@router.get('/expiry-board')
def expiry_board(_user=Depends(current_user)):
    cutoff = datetime.now(LOCAL_ZONE).date() + timedelta(days=30)
    rows, _ = query(f'''SELECT s.id,s.store_code,s.product_name,s.expiry_date,s.on_hand,s.substitute_name,
                       a.action,a.note FROM inventory_snapshots s LEFT JOIN expiry_actions a ON a.snapshot_id=s.id
                       WHERE {LATEST} AND s.expiry_date<=%s AND s.on_hand>0 ORDER BY s.expiry_date,s.id''', (cutoff,))
    return rows


@router.post('/expiry-board/{snapshot_id}', status_code=201)
def expiry_action(snapshot_id: int, data: ExpiryAction, user=Depends(INVENTORY)):
    cutoff = datetime.now(LOCAL_ZONE).date() + timedelta(days=30)
    with operational_transaction() as cur:
        cur.execute(f'SELECT s.id FROM inventory_snapshots s WHERE s.id=%s AND {LATEST} AND s.expiry_date<=%s AND s.on_hand>0 FOR UPDATE',
                    (snapshot_id, cutoff))
        if not cur.fetchone():
            raise HTTPException(409, '仅可处理最新在库且 30 天内到期或已过期的盘点')
        try:
            cur.execute('INSERT INTO expiry_actions(snapshot_id,action,note,actor_id) VALUES(%s,%s,%s,%s)',
                        (snapshot_id, data.action, data.note, user['sub']))
        except IntegrityError as error:
            if error.errno != 1062:
                raise
            raise HTTPException(409, '该临期快照已有处置记录') from error
        audit(cur, user['sub'], 'expiry_action', 'inventory_snapshot', snapshot_id, action=data.action, note=data.note)
    return {'action': data.action}


@router.get('/shortage-board')
def shortages(_user=Depends(current_user)):
    rows, _ = query(f'''SELECT s.id,s.store_code,s.sku,s.product_name,s.on_hand,r.reorder_point,
                       c.status,c.note,c.resolution FROM inventory_snapshots s
                       JOIN replenishment_recommendations r ON r.id=(SELECT MAX(id) FROM replenishment_recommendations WHERE snapshot_id=s.id)
                       LEFT JOIN shortage_cases c ON c.snapshot_id=s.id
                       WHERE ({LATEST} AND s.on_hand<r.reorder_point) OR c.snapshot_id IS NOT NULL
                       ORDER BY COALESCE(c.status='open',FALSE) DESC,s.id''')
    return rows


@router.post('/shortage-board/{snapshot_id}', status_code=201)
def escalate(snapshot_id: int, data: Note, user=Depends(INVENTORY)):
    with operational_transaction() as cur:
        cur.execute(f'''SELECT s.id FROM inventory_snapshots s JOIN replenishment_recommendations r
                     ON r.id=(SELECT MAX(id) FROM replenishment_recommendations WHERE snapshot_id=s.id)
                     WHERE s.id=%s AND {LATEST} AND s.on_hand<r.reorder_point FOR UPDATE''', (snapshot_id,))
        if not cur.fetchone():
            raise HTTPException(409, '请对库存不足的最新快照先运行预测')
        try:
            cur.execute('INSERT INTO shortage_cases(snapshot_id,note,opened_by) VALUES(%s,%s,%s)',
                        (snapshot_id, data.note, user['sub']))
        except IntegrityError as error:
            if error.errno != 1062:
                raise
            raise HTTPException(409, '该快照已登记缺货升级') from error
        audit(cur, user['sub'], 'shortage_escalated', 'inventory_snapshot', snapshot_id, note=data.note)
    return {'status': 'open'}


@router.put('/shortage-board/{snapshot_id}')
def resolve_shortage(snapshot_id: int, data: Note, user=Depends(MANAGER)):
    with operational_transaction() as cur:
        cur.execute('''UPDATE shortage_cases SET status='resolved',resolution=%s,resolved_by=%s,resolved_at=NOW()
                       WHERE snapshot_id=%s AND status='open' ''', (data.note, user['sub'], snapshot_id))
        if cur.rowcount != 1:
            raise HTTPException(409, '没有待关闭的缺货升级记录')
        audit(cur, user['sub'], 'shortage_resolved', 'inventory_snapshot', snapshot_id, resolution=data.note)
    return {'status': 'resolved'}


def shift_for_hour(hour):
    """把订阅发送小时归入门店班次：凌晨与上午归早班，中午下午归中班，晚间归晚班。"""
    if hour < 12:
        return 'morning'
    if hour < 18:
        return 'afternoon'
    return 'night'


def report_payload(cur, store_code, report_date):
    """生成门店在某一日的报表快照：每个 SKU 取 report_date 当天为止的最新盘点。"""
    cur.execute('''SELECT s.sku,s.product_name,s.snapshot_date,s.on_hand,r.reorder_point,r.recommended_qty,r.status
                  FROM inventory_snapshots s LEFT JOIN replenishment_recommendations r
                  ON r.id=(SELECT MAX(id) FROM replenishment_recommendations WHERE snapshot_id=s.id)
                  WHERE s.store_code=%s AND s.snapshot_date<=%s
                  AND NOT EXISTS (SELECT 1 FROM inventory_snapshots newer WHERE newer.store_code=s.store_code
                  AND newer.sku=s.sku AND newer.snapshot_date>s.snapshot_date AND newer.snapshot_date<=%s) ORDER BY s.sku''',
                (store_code, report_date, report_date))
    return cur.fetchall()


def freeze_report_version(cur, store_code, report_date):
    """当天首次为门店生成报表时冻结一版；同店后续所有班次都复用它。"""
    cur.execute('SELECT id,payload FROM report_versions WHERE store_code=%s AND report_date=%s FOR UPDATE',
                (store_code, report_date))
    existing = cur.fetchone()
    if existing:
        return existing['id'], existing['payload'], False
    payload = json.dumps(report_payload(cur, store_code, report_date), ensure_ascii=False, default=str)
    try:
        cur.execute('INSERT INTO report_versions(store_code,report_date,payload) VALUES(%s,%s,%s)',
                    (store_code, report_date, payload))
        version_id = cur.lastrowid
    except IntegrityError as error:
        if error.errno != 1062:
            raise
        # 另一个 worker 抢先冻结了当天的版本，复用它的内容，保证各班次口径一致。
        cur.execute('SELECT id,payload FROM report_versions WHERE store_code=%s AND report_date=%s FOR UPDATE',
                    (store_code, report_date))
        existing = cur.fetchone()
        if existing is None:
            raise HTTPException(409, '报表版本正在生成，请稍后重试') from error
        return existing['id'], existing['payload'], False
    return version_id, payload, True


def day_subscriptions(cur, day):
    """报表日当天生效的门店订阅（含当日较晚的班次）。

    订阅创建日不发送；暂停/恢复按生效历史判断，均只在次日报表上体现。
    """
    cur.execute('''SELECT rp.id,rp.store_code,rp.recipient_id,rp.delivery_hour,
                   COALESCE(u.name,'收件人') AS recipient_name,
                   COALESCE((SELECT c.enabled FROM report_schedule_changes c
                             WHERE c.subscription_id=rp.id AND c.effective_from<=%s
                             ORDER BY c.effective_from DESC,c.id DESC LIMIT 1),rp.enabled) AS enabled
                   FROM report_subscriptions rp JOIN users u ON u.id=rp.recipient_id
                   WHERE rp.created_on<%s
                   ORDER BY rp.store_code,rp.id''', (day, day))
    return [row for row in cur.fetchall() if bool(row['enabled'])]


def stores_due_at(cur, day, hour):
    """在 day 的 hour 点之前已有到点订阅的门店，用于决定何时首次冻结当天报表。"""
    cur.execute('''SELECT DISTINCT rp.store_code FROM report_subscriptions rp
                   WHERE rp.created_on<%s AND rp.delivery_hour<=%s
                   AND COALESCE((SELECT c.enabled FROM report_schedule_changes c
                                 WHERE c.subscription_id=rp.id AND c.effective_from<=%s
                                 ORDER BY c.effective_from DESC,c.id DESC LIMIT 1),rp.enabled)=TRUE
                   ORDER BY rp.store_code''', (day, hour, day))
    return [row['store_code'] for row in cur.fetchall()]


def freeze_shift_slots(cur, version_id, subscriptions):
    """冻结当天的班次名单：班次、收件人、发送小时一次性落库，之后改订阅从次日生效。

    以唯一键保证重复执行幂等，也便于进程中断后补齐未落库的班次。
    """
    created = 0
    for subscription in subscriptions:
        try:
            cur.execute('''INSERT INTO report_shift_slots(version_id,subscription_id,recipient_id,recipient_name,
                           delivery_hour,shift_name) VALUES(%s,%s,%s,%s,%s,%s)''',
                        (version_id, subscription['id'], subscription['recipient_id'],
                         subscription['recipient_name'], subscription['delivery_hour'],
                         shift_for_hour(subscription['delivery_hour'])))
            created += 1
        except IntegrityError as error:
            if error.errno != 1062:
                raise
    return created


def ensure_report_version(cur, store_code, report_date, subscriptions):
    version_id, _payload, _created = freeze_report_version(cur, store_code, report_date)
    freeze_shift_slots(cur, version_id, subscriptions)
    return version_id


def deliver_slot(cur, slot):
    """投递一个已冻结班次；唯一键保证重试不会重复投递，中断恢复后只补未发送的。"""
    cur.execute('SELECT payload FROM report_versions WHERE id=%s', (slot['version_id'],))
    payload = cur.fetchone()['payload']
    try:
        cur.execute('''INSERT INTO report_deliveries(subscription_id,report_date,payload)
                       VALUES(%s,%s,%s)''', (slot['subscription_id'], slot['report_date'], payload))
        delivery_id = cur.lastrowid
    except IntegrityError as error:
        if error.errno != 1062:
            raise
        cur.execute('''SELECT id FROM report_deliveries WHERE subscription_id=%s AND report_date=%s FOR UPDATE''',
                    (slot['subscription_id'], slot['report_date']))
        delivery_id = cur.fetchone()['id']
    cur.execute("UPDATE report_shift_slots SET status='sent',sent_at=NOW(),delivery_id=%s WHERE id=%s AND status='pending'",
                (delivery_id, slot['id']))


def deliver_due_reports(now=None):
    """按上海时间调度报表：先冻结当天版本与班次名单，再投递到点的班次。

    每个 store/day 的第一份订阅到达时就冻结版本；当天其余班次共用这一版。
    已冻结但未投递的班次（含昨夜中断遗留的）按原 report_date 和原班次补发。
    """
    now = now or datetime.now(LOCAL_ZONE)
    today = now.date()
    with operational_transaction() as cur:
        # 1) 今天的门店需要报表：到期订阅对应的门店先冻结版本和班次名单。
        #    班次名单取当天全部生效订阅（含较晚的班次），版本只在首次生成时冻结。
        due_stores = stores_due_at(cur, today, now.hour)
        # 2) 昨夜中断后已冻结但还没投递的班次，也要把所属门店的班次名单补齐。
        cur.execute('''SELECT DISTINCT v.store_code FROM report_shift_slots sl
                       JOIN report_versions v ON v.id=sl.version_id
                       WHERE sl.status='pending' AND v.report_date=%s''', (today,))
        for row in cur.fetchall():
            if row['store_code'] not in due_stores:
                due_stores.append(row['store_code'])
        if due_stores:
            subscriptions = day_subscriptions(cur, today)
            for store_code in due_stores:
                ensure_report_version(cur, store_code, today,
                                      [item for item in subscriptions if item['store_code'] == store_code])
        # 3) 已冻结的班次只要到点且未投递就补发：过去的报表日整天视为到点，
        #    今天的只取本小时以前；按原 report_date、原班次、原冻结版本投递。
        cur.execute('''SELECT sl.id,sl.version_id,sl.subscription_id,sl.delivery_hour,sl.shift_name,v.report_date
                       FROM report_shift_slots sl JOIN report_versions v ON v.id=sl.version_id
                       WHERE sl.status='pending'
                       AND (v.report_date<%s OR (v.report_date=%s AND sl.delivery_hour<=%s))
                       ORDER BY v.report_date,sl.delivery_hour,sl.id LIMIT 50 FOR UPDATE SKIP LOCKED''',
                    (today, today, now.hour))
        for slot in cur.fetchall():
            deliver_slot(cur, slot)


@router.get('/report-subscriptions')
def subscriptions(user=Depends(current_user)):
    manager = user['role'] == 'regional_manager'
    subscriptions, _ = query('''SELECT rp.*,u.name AS recipient FROM report_subscriptions rp JOIN users u ON u.id=rp.recipient_id
                               WHERE %s OR u.id=%s ORDER BY rp.id''', (manager, user['sub']))
    deliveries, _ = query('''SELECT d.id,d.report_date,d.delivered_at,d.payload,rp.store_code,u.name AS recipient,
                             sl.shift_name,sl.delivery_hour FROM report_deliveries d
                             JOIN report_subscriptions rp ON rp.id=d.subscription_id
                             JOIN users u ON u.id=rp.recipient_id
                             LEFT JOIN report_shift_slots sl ON sl.delivery_id=d.id
                             WHERE %s OR u.id=%s ORDER BY d.id DESC LIMIT 60''', (manager, user['sub']))
    for row in deliveries:
        row['payload'] = json.loads(row['payload'])
    stores, _ = query('SELECT DISTINCT store_code,store_name FROM inventory_snapshots ORDER BY store_code') if manager else ([], 0)
    recipients, _ = query('SELECT id,name FROM users ORDER BY id') if manager else ([], 0)
    return {'subscriptions': subscriptions, 'deliveries': deliveries, 'stores': stores, 'recipients': recipients}


@router.get('/report-reconciliation')
def report_reconciliation(store_code: str = Query(min_length=1, max_length=40),
                          report_date: date | None = Query(default=None), user=Depends(MANAGER)):
    """经理按门店和日期对账：当天冻结版本与各班次是否已送。"""
    report_date = report_date or datetime.now(LOCAL_ZONE).date()
    empty_shifts = {name: {'total': 0, 'sent': 0, 'pending': 0} for name in SHIFT_NAMES}
    version, _ = query('SELECT id,store_code,report_date,frozen_at,payload FROM report_versions WHERE store_code=%s AND report_date=%s',
                       (store_code, report_date))
    if not version:
        return {'store_code': store_code, 'report_date': str(report_date), 'version': None,
                'slots': [], 'shifts': empty_shifts}
    slots, _ = query('''SELECT sl.id,sl.recipient_name,sl.delivery_hour,sl.shift_name,sl.status,sl.sent_at,
                        d.delivered_at FROM report_shift_slots sl
                        LEFT JOIN report_deliveries d ON d.id=sl.delivery_id
                        WHERE sl.version_id=%s ORDER BY sl.delivery_hour,sl.id''', (version[0]['id'],))
    shifts = {}
    for name in SHIFT_NAMES:
        group = [slot for slot in slots if slot['shift_name'] == name]
        shifts[name] = {
            'total': len(group),
            'sent': sum(1 for slot in group if slot['status'] == 'sent'),
            'pending': sum(1 for slot in group if slot['status'] == 'pending'),
        }
    payload = json.loads(version[0]['payload'])
    version[0]['payload'] = payload
    return {'store_code': store_code, 'report_date': str(report_date), 'version': version[0],
            'slots': slots, 'shifts': shifts}


@router.post('/report-subscriptions', status_code=201)
def create_subscription(data: Subscription, user=Depends(MANAGER)):
    with operational_transaction() as cur:
        cur.execute('SELECT id FROM inventory_snapshots WHERE store_code=%s LIMIT 1', (data.store_code,))
        if not cur.fetchone():
            raise HTTPException(404, '门店不存在')
        cur.execute('SELECT id FROM users WHERE id=%s', (data.recipient_id,))
        if not cur.fetchone():
            raise HTTPException(404, '收件人不存在')
        try:
            cur.execute('''INSERT INTO report_subscriptions(store_code,recipient_id,delivery_hour,created_on)
                           VALUES(%s,%s,%s,%s)''', (data.store_code, data.recipient_id, data.delivery_hour,
                                                  datetime.now(LOCAL_ZONE).date()))
        except IntegrityError as error:
            if error.errno != 1062:
                raise
            raise HTTPException(409, '该门店、收件人和时间已有订阅') from error
        identifier = cur.lastrowid
        # 订阅从创建次日起生效，记录一条基线，后续暂停/恢复只影响其后的日期。
        cur.execute('''INSERT INTO report_schedule_changes(subscription_id,effective_from,enabled)
                       VALUES(%s,%s,TRUE)''',
                    (identifier, datetime.now(LOCAL_ZONE).date() + timedelta(days=1)))
        audit(cur, user['sub'], 'report_subscribed', 'report_subscription', identifier, store=data.store_code)
    return {'id': identifier, 'effective_from': str(datetime.now(LOCAL_ZONE).date() + timedelta(days=1))}


@router.put('/report-subscriptions/{subscription_id}')
def toggle_subscription(subscription_id: int, data: Enabled, user=Depends(MANAGER)):
    """暂停或恢复订阅从次日生效，当天已冻结的班次名单保持不变。"""
    with operational_transaction() as cur:
        cur.execute('SELECT id,enabled FROM report_subscriptions WHERE id=%s FOR UPDATE', (subscription_id,))
        current = cur.fetchone()
        if not current:
            raise HTTPException(404, '订阅不存在')
        if bool(current['enabled']) != data.enabled:
            cur.execute('UPDATE report_subscriptions SET enabled=%s WHERE id=%s', (data.enabled, subscription_id))
            effective = datetime.now(LOCAL_ZONE).date() + timedelta(days=1)
            cur.execute('''INSERT INTO report_schedule_changes(subscription_id,effective_from,enabled)
                           VALUES(%s,%s,%s) ON DUPLICATE KEY UPDATE enabled=VALUES(enabled)''',
                        (subscription_id, effective, data.enabled))
            audit(cur, user['sub'], 'report_schedule_updated', 'report_subscription', subscription_id,
                  enabled=data.enabled, effective_from=str(effective))
    return {'enabled': data.enabled, 'effective_from': str(datetime.now(LOCAL_ZONE).date() + timedelta(days=1))}


@router.get('/receipt-audit')
def receipt_audit(_user=Depends(current_user)):
    rows, _ = query(f'''SELECT d.id,d.recipient_name,d.shift,d.status,d.dispatched_at,d.received_at,
                       a.store_code,a.approved_qty,e.reason,e.closed_at,({RECEIPT_GAP}) AS overdue
                       FROM dispatch_records d JOIN approval_orders a ON a.id=d.approval_id
                       LEFT JOIN receipt_exceptions e ON e.dispatch_id=d.id
                       WHERE d.status IN ('sent','received') ORDER BY overdue DESC,d.id DESC LIMIT 120''')
    return rows


@router.post('/receipt-audit/{dispatch_id}', status_code=201)
def flag_receipt(dispatch_id: int, data: Note, user=Depends(SHIFT)):
    with operational_transaction() as cur:
        cur.execute(f'SELECT d.id FROM dispatch_records d WHERE d.id=%s AND {RECEIPT_GAP} FOR UPDATE', (dispatch_id,))
        if not cur.fetchone():
            raise HTTPException(409, '只有超过 2 小时未签收或缺失发送时间的分发可以登记异常')
        try:
            cur.execute('INSERT INTO receipt_exceptions(dispatch_id,reason,opened_by) VALUES(%s,%s,%s)',
                        (dispatch_id, data.note, user['sub']))
        except IntegrityError as error:
            if error.errno != 1062:
                raise
            raise HTTPException(409, '该分发已有异常记录') from error
        audit(cur, user['sub'], 'receipt_flagged', 'dispatch', dispatch_id, reason=data.note)
    return {'status': 'open'}


@router.put('/receipt-audit/{dispatch_id}')
def close_receipt(dispatch_id: int, user=Depends(SHIFT)):
    with operational_transaction() as cur:
        cur.execute('''UPDATE receipt_exceptions e JOIN dispatch_records d ON d.id=e.dispatch_id
                       SET e.closed_at=NOW(),e.closed_by=%s WHERE d.id=%s AND d.status='received'
                       AND d.received_at IS NOT NULL AND e.closed_at IS NULL''', (user['sub'], dispatch_id))
        if cur.rowcount != 1:
            raise HTTPException(409, '需先确认收件，再结清待跟进异常')
        audit(cur, user['sub'], 'receipt_reconciled', 'dispatch', dispatch_id)
    return {'status': 'closed'}
