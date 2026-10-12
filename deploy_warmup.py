"""Deployment-scoped display warm-up, based on verified forward LIVE forecasts.

The engine always forecasts and records results. Public UI predictions are
hidden until 30 distinct, pre-outcome LIVE forecasts on that board have settled
since the current Railway deployment began. This is a display policy, not a
measure of statistical significance or accuracy.
"""
import os
import time
import sqlite3

TARGET = 30
DEPLOYMENT_KEY = (os.environ.get('RAILWAY_DEPLOYMENT_ID')
                  or os.environ.get('RAILWAY_GIT_COMMIT_SHA')
                  or f'process-{os.getpid()}-{int(time.time())}')


def ensure_schema(con):
    con.executescript('''
    CREATE TABLE IF NOT EXISTS display_warmup_deployments (
        deployment_key TEXT PRIMARY KEY,
        started_at REAL NOT NULL
    );
    CREATE TABLE IF NOT EXISTS display_warmup_settled (
        deployment_key TEXT NOT NULL,
        board TEXT NOT NULL,
        external_key TEXT NOT NULL,
        settled_at REAL NOT NULL,
        PRIMARY KEY (deployment_key, board, external_key)
    );
    CREATE INDEX IF NOT EXISTS idx_display_warmup_board
        ON display_warmup_settled(deployment_key, board);
    ''')
    con.execute('INSERT OR IGNORE INTO display_warmup_deployments(deployment_key,started_at) VALUES(?,?)',
                (DEPLOYMENT_KEY, time.time()))


def started_at(con):
    row=con.execute('SELECT started_at FROM display_warmup_deployments WHERE deployment_key=?',
                    (DEPLOYMENT_KEY,)).fetchone()
    return float(row[0]) if row else time.time()


def mark_forward_live(con, board, key, issued_at, settled_at):
    """Count ONLY a forecast issued since deploy started, before settlement."""
    if float(issued_at or 0) < started_at(con) or float(issued_at or 0) >= float(settled_at):
        return False
    cur=con.execute('INSERT OR IGNORE INTO display_warmup_settled '
                    '(deployment_key,board,external_key,settled_at) VALUES(?,?,?,?)',
                    (DEPLOYMENT_KEY, str(board), str(key), float(settled_at)))
    return cur.rowcount == 1


def progress(con, board):
    n=int(con.execute('SELECT COUNT(*) FROM display_warmup_settled '
                      'WHERE deployment_key=? AND board=?',
                      (DEPLOYMENT_KEY, str(board))).fetchone()[0])
    count=min(n, TARGET)
    return {'warmup_count':count,'warmup_target':TARGET,'warmup_remaining':max(0,TARGET-count),
            'warmup_ready':count >= TARGET,'warmup_state':'ready' if n>=TARGET else 'warming'}
