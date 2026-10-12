"""V25 Sunwin next-round hypothesis engine.

Uses *only prior settled rounds* for candidate signals; adapts modestly from
pre-outcome LIVE expert scores. Does not claim to predict independent random dice
better than chance. Keeps V15 public function names for maychubot.py compatibility.
"""
import json
import math

T, X = 'TÀI', 'XỈU'
VERSION = 'V25'
NAMES = ('FOLLOW', 'REVERSE', 'LAST24', 'MARKOV1', 'MARKOV2', 'RUN_STATE')
_BASE = {'FOLLOW': .65, 'REVERSE': .65, 'LAST24': 1.0,
         'MARKOV1': 1.10, 'MARKOV2': .90, 'RUN_STATE': 1.0}


def _context(seq):
    seq = [v for v in seq if v in (T, X)]
    if not seq:
        return {}, {}
    n = len(seq)
    last = seq[-1]
    recent = seq[-24:]
    count_t = recent.count(T)
    freq = T if count_t > len(recent) - count_t else X if count_t < len(recent) - count_t else last
    run = 1
    for item in reversed(seq[:-1]):
        if item != last:
            break
        run += 1

    counts = {}
    def conditional(order):
        if n <= order:
            counts[order] = 0
            return freq
        pattern = seq[-order:]
        a = b = 0
        for j in range(max(order, n - 240), n):
            if seq[j - order:j] == pattern:
                if seq[j] == T:
                    a += 1
                else:
                    b += 1
        support = a + b
        counts[order] = support
        # If this exact context is unproven, keep the vote but reduce its weight.
        if support < 8 or abs(a - b) <= 1:
            return freq
        return T if a > b else X

    votes = {'FOLLOW': last, 'REVERSE': X if last == T else T,
             'LAST24': freq, 'MARKOV1': conditional(1),
             'MARKOV2': conditional(2),
             'RUN_STATE': X if run >= 4 and last == T else T if run >= 4 else last}
    supports = {'MARKOV1': counts.get(1, 0), 'MARKOV2': counts.get(2, 0)}
    return votes, supports


def candidates(seq):
    return _context(seq)[0]


def weights(seq, stats=None):
    """Non-leaky, reliability-limited expert weights for transparent UI/debugging."""
    candidates_now, supports = _context(seq)
    stats = stats or {}
    out = {}
    for name, prediction in candidates_now.items():
        obs = list(stats.get(name) or [])[-160:]
        n = len(obs)
        correct = sum(1 for x in obs if x)
        # Shrink strongly to the 50% baseline; only use samples verified LIVE.
        rate = (correct + 18.0) / (n + 36.0)
        adaptive = max(.82, min(1.18, 1.0 + (rate - .5) * 2.0)) if n >= 36 else 1.0
        enough = 1.0
        if name == 'MARKOV1':
            enough = min(1.0, max(.12, supports.get('MARKOV1', 0) / 14.0))
        elif name == 'MARKOV2':
            enough = min(1.0, max(.12, supports.get('MARKOV2', 0) / 10.0))
        final = _BASE[name] * enough * adaptive
        out[name] = {'prediction': prediction, 'n': n, 'correct': correct,
                     'historical_rate': round(100 * correct / n, 1) if n else None,
                     'weight': round(final, 4),
                     'context_samples': supports.get(name) if name.startswith('MARKOV') else None}
    return out


def choose(seq, stats=None):
    scores = weights(seq, stats)
    if not scores:
        return {'prediction': T, 'strategy': 'COLD_START', 'model_scores': {},
                'agreement': .5, 'engine': VERSION, 'evidence': 'NO_HISTORY',
                'signal_is_win_probability': False}
    total = {T: 0.0, X: 0.0}
    for data in scores.values():
        total[data['prediction']] += float(data['weight'])
    fallback = candidates(seq)['LAST24']
    outcome = T if total[T] > total[X] else X if total[X] > total[T] else fallback
    summed = total[T] + total[X]
    # This is weighted agreement, not winning probability.
    agreement = round(total[outcome] / summed, 3) if summed else .5
    return {'prediction': outcome, 'strategy': 'V25_CAUSAL_WEIGHTED_EXPERTS',
            'model_scores': scores, 'agreement': agreement,
            'engine': VERSION, 'evidence': 'LIVE_ONLY_UNVALIDATED',
            'signal_is_win_probability': False}


def ensure_schema(con):
    # New versioned tables: old V15/V24 labels are preserved, never fed into V25.
    con.executescript('''
    CREATE TABLE IF NOT EXISTS v25_issued_experts (
        board TEXT NOT NULL, base_key TEXT NOT NULL,
        forecasts_json TEXT NOT NULL, created_at REAL NOT NULL,
        PRIMARY KEY(board, base_key)
    );
    CREATE TABLE IF NOT EXISTS v25_expert_log (
        board TEXT NOT NULL, external_key TEXT NOT NULL,
        strategy TEXT NOT NULL, prediction TEXT NOT NULL,
        actual TEXT NOT NULL, ok INTEGER NOT NULL,
        source TEXT NOT NULL CHECK(source IN ('live','replay')),
        created_at REAL NOT NULL,
        PRIMARY KEY(board,external_key,strategy)
    );
    CREATE INDEX IF NOT EXISTS idx_v25_expert_board
        ON v25_expert_log(board, strategy, source, external_key);
    ''')


def load_stats(con, board, limit=160):
    out = {name: [] for name in NAMES}
    for name in NAMES:
        rows = con.execute('''SELECT ok FROM v25_expert_log
           WHERE board=? AND strategy=? AND source='live'
           ORDER BY CAST(external_key AS INTEGER) DESC LIMIT ?''',
           (board, name, limit)).fetchall()
        out[name] = [int(row[0]) for row in reversed(rows)]
    return out


def seal_forecast(con, board, base_key, preds, issued_time):
    valid = {name: side for name, side in preds.items() if name in NAMES and side in (T, X)}
    if valid:
        con.execute('INSERT OR IGNORE INTO v25_issued_experts '
                    '(board,base_key,forecasts_json,created_at) VALUES(?,?,?,?)',
                    (board, str(base_key), json.dumps(valid, ensure_ascii=False), float(issued_time)))


def issued_forecast(con, board, base_key, settled_time):
    row = con.execute('SELECT forecasts_json,created_at FROM v25_issued_experts '
                      'WHERE board=? AND base_key=?', (board, str(base_key))).fetchone()
    if not row or float(row[1]) >= float(settled_time):
        return None
    try:
        p = json.loads(row[0])
        return {k: v for k, v in p.items() if k in NAMES and v in (T, X)}
    except (ValueError, TypeError):
        return None


def save_result(con, board, key, preds, actual, is_live, settled_time):
    if actual not in (T, X):
        return
    source = 'live' if is_live else 'replay'
    con.executemany('''INSERT OR IGNORE INTO v25_expert_log
        (board,external_key,strategy,prediction,actual,ok,source,created_at)
        VALUES(?,?,?,?,?,?,?,?)''',
        [(board, str(key), name, side, actual, int(side == actual), source, float(settled_time))
         for name, side in preds.items() if name in NAMES and side in (T, X)])


def audit(con, board='sunwin:hu'):
    rows = con.execute('''SELECT source,COUNT(DISTINCT external_key),SUM(ok)
        FROM v25_expert_log WHERE board=? AND strategy='LAST24'
        GROUP BY source''', (board,)).fetchall()
    return {row[0]: {'rounds': int(row[1]), 'correct': int(row[2] or 0)} for row in rows}


def bootstrap(con, board='sunwin:hu'):
    if con.execute('SELECT 1 FROM v25_expert_log WHERE board=? LIMIT 1', (board,)).fetchone():
        return 0
    rows = con.execute('''SELECT external_key,result,seen_at FROM rounds
       WHERE board=? ORDER BY CAST(external_key AS INTEGER),id''', (board,)).fetchall()
    history = []
    prev = None
    n = 0
    for r in rows:
        key = str(r['external_key'])
        actual = r['result']
        if prev is not None and (not prev.isdigit() or not key.isdigit() or int(key) != int(prev) + 1):
            history = []
        if len(history) >= 12 and actual in (T, X):
            save_result(con, board, key, candidates(history), actual, False, float(r['seen_at'] or 0))
            n += 1
        if actual in (T, X):
            history.append(actual)
        prev = key
    return n
