"""V15 Sunwin: auditable causal, low-correlation next-round hypotheses.

Always returns TÀI/XỈU. This module does not claim a statistical edge against
independent random dice. It records individual expert forecasts before their
outcome and never mistakes historical replay for verified LIVE performance.
"""
import math
import json
from collections import Counter

T, X = 'TÀI', 'XỈU'
VERSION='V15'
NAMES=('FOLLOW','REVERSE','LAST24','MARKOV1','MARKOV2','RUN_STATE')


def candidates(seq):
    seq=[x for x in seq if x in (T,X)]
    if not seq: return {}
    tail=seq[-1]; n=len(seq)
    short=seq[-24:]
    t=short.count(T);x=len(short)-t
    frequency=T if t>x else X if x>t else tail
    def markov(order):
        # Beta(3,3) shrinkage, fixed lookback; no current/future outcome included.
        if n<order+15:return frequency
        context=seq[-order:]; a=b=3
        # historical outcomes only, including latest occurrence prefix
        for i in range(max(order,n-240),n):
            if seq[i-order:i]==context:
                if seq[i]==T:a+=1
                else:b+=1
        if a+b < 16 or abs(a-b) < 3:return frequency
        return T if a>b else X
    run=1
    for v in reversed(seq[:-1]):
        if v!=tail:break
        run+=1
    return {'FOLLOW':tail,'REVERSE':X if tail==T else T,
            'LAST24':frequency,'MARKOV1':markov(1),'MARKOV2':markov(2),
            'RUN_STATE':(X if tail==T else T) if run>=4 else tail}


def choose(seq, stats=None):
    """Online shrunk expert vote, max one vote per independent hypothesis.

    stats[nm] is list of 0/1 correctness from older prequential forecasts,
    ordered from oldest to newest. Conservative weights cannot claim real odds.
    """
    d=candidates(seq)
    if not d:return {'prediction':T,'strategy':'COLD','model_scores':{},'agreement':.5,'engine':'V15','evidence':'NO_HISTORY'}
    stats=stats or {}
    vote={T:0.,X:0.};sc={}
    for name,pred in d.items():
        obs=list(stats.get(name) or [])[-160:]
        n=len(obs);w=sum(obs)
        posterior=(w+12.)/(n+24.)
        # Strong prior for 50%; cap possible effect of noisy historic streaks.
        reliability=min(1.,n/80.)
        weight=max(.65,min(1.35,1.+1.3*reliability*(posterior-.5)))
        vote[pred]+=weight
        sc[name]={'n':n,'correct':w,'historical_rate':round(w/n*100,1) if n else None,
                  'weight':round(weight,3),'prediction':pred}
    fallback=d['LAST24'];pred=T if vote[T]>vote[X] else X if vote[X]>vote[T] else fallback
    total=vote[T]+vote[X]
    agree=vote[pred]/total if total else .5
    return {'prediction':pred,'strategy':'SIX_EXPERT_SHRUNK_VOTE',
            'model_scores':sc,'agreement':round(agree,3),'engine':'V15',
            'evidence':'LIVE_ONLY_NOT_VALIDATED',
            'signal_is_win_probability':False}


def ensure_schema(con):
    con.executescript('''
    CREATE TABLE IF NOT EXISTS v15_issued_experts (
      board TEXT NOT NULL, base_key TEXT NOT NULL, forecasts_json TEXT NOT NULL,
      created_at REAL NOT NULL, PRIMARY KEY(board,base_key)
    );
    CREATE TABLE IF NOT EXISTS v15_expert_log (
      board TEXT NOT NULL, external_key TEXT NOT NULL, strategy TEXT NOT NULL,
      prediction TEXT NOT NULL, actual TEXT NOT NULL, ok INTEGER NOT NULL,
      source TEXT NOT NULL CHECK(source IN ('live','replay')),
      created_at REAL NOT NULL,
      PRIMARY KEY(board,external_key,strategy)
    );
    CREATE INDEX IF NOT EXISTS idx_v15_expert_board
      ON v15_expert_log(board,strategy,created_at);
    ''')


def load_stats(con,board,limit=160):
    out={name:[] for name in NAMES}
    for name in NAMES:
        # Weight experts only from forecasts truly issued before settlement.
        # REPLAY is kept for diagnostics but must not train the live selector.
        rows=con.execute('''SELECT ok FROM v15_expert_log
          WHERE board=? AND strategy=? AND source='live'
          ORDER BY CAST(external_key AS INTEGER) DESC LIMIT ?''',
          (board,name,limit)).fetchall()
        out[name]=[int(x[0]) for x in reversed(rows)]
    return out


def seal_forecast(con,board,base_key,preds,issued_time):
    """Immutable, stamped BEFORE result. Reboots cannot rewrite shadow experts."""
    valid={k:v for k,v in preds.items() if k in NAMES and v in (T,X)}
    if not valid:return
    con.execute('INSERT OR IGNORE INTO v15_issued_experts '
        '(board,base_key,forecasts_json,created_at) VALUES(?,?,?,?)',
        (board,str(base_key),json.dumps(valid,ensure_ascii=False),float(issued_time)))


def issued_forecast(con,board,base_key,settled_time):
    row=con.execute('SELECT forecasts_json,created_at FROM v15_issued_experts '
        'WHERE board=? AND base_key=?',(board,str(base_key))).fetchone()
    if not row or float(row[1]) >= float(settled_time):return None
    try:
        preds=json.loads(row[0]);return {k:v for k,v in preds.items() if k in NAMES and v in (T,X)}
    except (ValueError,TypeError):return None


def save_result(con,board,key,preds,actual,is_live,settled_time):
    if actual not in (T,X):return
    source='live' if is_live else 'replay'
    con.executemany('''INSERT OR IGNORE INTO v15_expert_log
    (board,external_key,strategy,prediction,actual,ok,source,created_at)
    VALUES(?,?,?,?,?,?,?,?)''',
     [(board,str(key),nm,p,actual,int(p==actual),source,float(settled_time))
       for nm,p in preds.items() if nm in NAMES and p in (T,X)])


def audit(con,board='sunwin:hu'):
    rows=con.execute('''SELECT source,COUNT(DISTINCT external_key), SUM(ok) FROM v15_expert_log
        WHERE board=? AND strategy=? GROUP BY source''',(board,'LAST24')).fetchall()
    counts={r[0]:{'rounds':int(r[1]),'correct':int(r[2] or 0)} for r in rows}
    return counts


def bootstrap(con, board='sunwin:hu'):
    """One-time prequential replay with gap-aware reset; replay NEVER counted LIVE.

    Historical rows may be ordered and evaluated causally but do not establish
    what the production server truly forecast before actual settlement.
    """
    check=con.execute("SELECT 1 FROM v15_expert_log WHERE board=? LIMIT 1",(board,)).fetchone()
    if check:return 0
    rows=con.execute('''SELECT external_key,result,seen_at FROM rounds WHERE board=?
                          ORDER BY CAST(external_key AS INTEGER),id''',(board,)).fetchall()
    hist=[];last=None;n=0
    for r in rows:
        key=str(r['external_key']);actual=r['result']
        if last is not None and (not key.isdigit() or not last.isdigit() or int(key)!=int(last)+1):
            hist=[]
        if len(hist)>=12 and actual in (T,X):
            preds=candidates(hist)
            save_result(con,board,key,preds,actual,False,float(r['seen_at'] or 0))
            n+=1
        if actual in (T,X):hist.append(actual)
        last=key
    return n
