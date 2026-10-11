"""Non-destructive API/SQLite consistency guard for CLASSIC V8.

Does not pretend old conflicting outcomes can be reconstructed. All rejected
observations are audited; confirmed rounds and predictions are never rewritten.
"""
import hashlib
import json
import re
import time

SID_RE = re.compile(r'^\d+$')


def canonical(meta):
    meta = meta if isinstance(meta, dict) else {}
    vals = meta.get('dice') or []
    dice = []
    if isinstance(vals, (list, tuple)):
        try:
            dice = sorted(int(v) for v in vals)
        except (ValueError, TypeError):
            return None, 'dice_not_numeric'
    if dice and (len(dice) != 3 or any(not 1 <= d <= 6 for d in dice)):
        return None, 'dice_invalid'
    total = meta.get('total')
    if total is not None:
        try:
            if isinstance(total, bool) or str(int(total)) != str(total).strip():
                return None, 'total_invalid'
            total = int(total)
        except (ValueError, TypeError):
            return None, 'total_invalid'
        if not 3 <= total <= 18:
            return None, 'total_out_of_range'
    if dice and total is not None and sum(dice) != total:
        return None, 'dice_sum_mismatch'
    if dice and total is None:
        total = sum(dice)
    return {'dice': dice, 'total': total}, None


def same_observation(board, left, right):
    if left.get('result') != right.get('result'):
        return False
    if not board.startswith('sunwin:'):
        return True
    a, err_a = canonical(left.get('meta'))
    b, err_b = canonical(right.get('meta'))
    if err_a or err_b:
        return False
    if a['total'] is not None and b['total'] is not None and a['total'] != b['total']:
        return False
    if a['dice'] and b['dice'] and a['dice'] != b['dice']:
        return False
    return True


def validate(board, item):
    key = str(item.get('key', '')).strip()
    if not key or key == 'None' or len(key) > 128:
        return 'missing_or_invalid_round_id'
    result = item.get('result')
    if result not in ('TÀI', 'XỈU'):
        return 'invalid_result'
    if board.startswith('sunwin:'):
        if not SID_RE.fullmatch(key):
            return 'unstable_sunwin_session_id'
        calc, error = canonical(item.get('meta'))
        if error:
            return error
        total = calc['total']
        if total is not None and result != ('TÀI' if total >= 11 else 'XỈU'):
            return 'result_disagrees_with_dice'
    return None


def ensure_schema(con):
    con.executescript('''
    CREATE TABLE IF NOT EXISTS v8_source_observations(
      board TEXT NOT NULL, external_key TEXT NOT NULL, source TEXT NOT NULL,
      result TEXT NOT NULL, meta_json TEXT NOT NULL,
      first_seen REAL NOT NULL, last_seen REAL NOT NULL, checks INTEGER NOT NULL DEFAULT 1,
      PRIMARY KEY(board, external_key, source)
    );
    CREATE INDEX IF NOT EXISTS idx_v8_src_board ON v8_source_observations(board,external_key);
    CREATE TABLE IF NOT EXISTS v8_data_conflicts(
      id INTEGER PRIMARY KEY AUTOINCREMENT, board TEXT NOT NULL,
      external_key TEXT NOT NULL, source TEXT NOT NULL,
      reason TEXT NOT NULL, fingerprint TEXT NOT NULL UNIQUE,
      stored_json TEXT, incoming_json TEXT,
      first_seen REAL NOT NULL, last_seen REAL NOT NULL,
      repeats INTEGER NOT NULL DEFAULT 1
    );
    CREATE INDEX IF NOT EXISTS idx_v8_conflicts_board ON v8_data_conflicts(board,id);
    ''')


def log_conflict(con, board, key, source, reason, stored, incoming, ts):
    before = json.dumps(stored, ensure_ascii=False, sort_keys=True, default=str)
    after = json.dumps(incoming, ensure_ascii=False, sort_keys=True, default=str)
    digest = hashlib.sha256(json.dumps([board,key,source,reason,before,after], ensure_ascii=False).encode()).hexdigest()
    con.execute('''INSERT INTO v8_data_conflicts
        (board,external_key,source,reason,fingerprint,stored_json,incoming_json,first_seen,last_seen)
        VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(fingerprint) DO UPDATE SET
        last_seen=excluded.last_seen,repeats=repeats+1''',
        (board,key,source,reason,digest,before[:1000],after[:1000],ts,ts))


def prepare(con, board, rows, source, now, last_key):
    """Admit ONLY new validated, monotone rows; log conflicting observations.

    Existing immutable round records are checked on every API poll. Records in
    the current batch are compared as well, so contradictory duplicates don't
    get silently reduced to one value by the normalizer.
    """
    stored = {}
    for r in con.execute('SELECT external_key,result,meta_json FROM rounds WHERE board=?', (board,)):
        try: meta = json.loads(r['meta_json'] or '{}')
        except (ValueError, TypeError): meta = {}
        stored[str(r['external_key'])] = {'result':r['result'], 'meta':meta}
    accepted = []
    new_ids = set()
    high = int(last_key) if board.startswith('sunwin:') and SID_RE.fullmatch(str(last_key or '')) else None
    rejected = 0
    stale = 0
    for item in rows:
        key = str(item.get('key', '')).strip()
        incoming = {'result':item.get('result'),'meta':item.get('meta') or {}}
        err = validate(board, item)
        if err:
            log_conflict(con,board,key,source,err,{},incoming,now)
            rejected += 1
            continue
        old = stored.get(key)
        if old and not same_observation(board,old,incoming):
            log_conflict(con,board,key,source,'immutable_round_disagrees',old,incoming,now)
            rejected += 1
            continue
        other = con.execute('''SELECT source,result,meta_json FROM v8_source_observations
            WHERE board=? AND external_key=?''',(board,key)).fetchall()
        mismatch = False
        for row in other:
            try:meta = json.loads(row['meta_json'])
            except (ValueError, TypeError):meta = {}
            previous = {'result':row['result'],'meta':meta}
            if not same_observation(board,previous,incoming):
                log_conflict(con,board,key,source,'api_sources_disagree:'+row['source'],previous,incoming,now)
                mismatch = True
        if mismatch:
            rejected += 1
            continue
        con.execute('''INSERT INTO v8_source_observations
            (board,external_key,source,result,meta_json,first_seen,last_seen,checks)
            VALUES(?,?,?,?,?,?,?,1) ON CONFLICT(board,external_key,source)
            DO UPDATE SET last_seen=excluded.last_seen,checks=checks+1''',
            (board,key,source,item['result'],json.dumps(incoming['meta'],ensure_ascii=False),now,now))
        if key in stored or key in new_ids:
            continue
        if high is not None and board.startswith('sunwin:') and int(key) < high:
            stale += 1
            continue
        accepted.append(item)
        new_ids.add(key)
        if high is not None and board.startswith('sunwin:'):
            high = max(high,int(key))
    return accepted, {'rejected':rejected, 'stale':stale}


def diagnostics(con, board=None):
    params=(board,) if board else ()
    where='WHERE board=?' if board else ''
    n=con.execute('SELECT COUNT(*) FROM v8_data_conflicts '+where,params).fetchone()[0]
    recent=con.execute('''SELECT board,external_key,source,reason,first_seen,repeats
        FROM v8_data_conflicts '''+where+' ORDER BY last_seen DESC LIMIT 8',params).fetchall()
    return {'conflicts':n,'recent':[dict(r) for r in recent]}
