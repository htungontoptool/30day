# DEVELOPER THANHTUNG · 24/7 LEARNING BACKEND + ADMIN BOT
# External HTML can be hosted anywhere. This process provides API/cache/learning.
# Run: python maychubot.py

import os, re, json, time, math, html, asyncio, sqlite3, hashlib, threading, io, ipaddress, secrets
from datetime import datetime, timezone, timedelta
from pathlib import Path
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

import httpx
from hoccauvip import STRATEGY_NAMES, strategy_predictions, ensemble_prediction
import nhandiencau as acl
import dataweb as guard
from md5_server_engine import HASH_RE, scores288, weighted_result
from live_signal_guard import gate_live
import deploy_warmup as warmup

BASE_DIR = Path(__file__).resolve().parent
PORT = int(os.getenv('PORT','8080'))
WEB_BOT_TOKEN = os.getenv('WEB_BOT_TOKEN','').strip()
ADMIN_IDS = {int(x) for x in os.getenv('ADMIN_IDS','').replace(';',',').split(',') if x.strip().lstrip('-').isdigit()}
DB_PATH = os.getenv('DB_PATH', str(BASE_DIR/'thanhtung_learning.db'))
CONFIG_PATH = Path(os.getenv('WEB_CONFIG_PATH', str(BASE_DIR/'web_config.json')))
BOT_POLL_TIMEOUT = max(5,int(os.getenv('BOT_POLL_TIMEOUT','20')))
MAX_BACKFILL_PER_BOARD=max(80,min(800,int(os.getenv('MAX_BACKFILL_PER_BOARD','320'))))

DEFAULT_CONFIG = {
    'brand':'DEVELOPER THANHTUNG',
    'sunwin_game_url':'https://play.sunwin.webcam/?affId=Sunwin',
    'sunwin_current_api':'https://amongst-plots-called-dining.trycloudflare.com/api/tx',
    'sunwin_current_fallback':'https://kwinstore.com/sunwin/tx/9b7a587deb56a4caf8de8ffdb0c13e8d22e793ae598b66c7',
    'sunwin_history_api':'https://kwinstore.com/sunwin/tx/history/9b7a587deb56a4caf8de8ffdb0c13e8d22e793ae598b66c7',
    'bcr_api':'https://bcrsexy.onrender.com/api/baccarat',
    'updated_at':0,
}

LOCK=threading.RLock()
CONFIG={}
CACHE={
    'sun_current':{'ok':False,'data':None,'ts':0,'error':'chưa tải'},
    'sun_history':{'ok':False,'data':None,'ts':0,'error':'chưa tải'},
    'bcr':{'ok':False,'data':None,'ts':0,'error':'chưa tải'},
}
LATEST={}
LEARN_STATUS={'started':time.time(),'last_cycle':0,'last_error':'','new_rounds':0,'last_new_round_at':0,'cycles':0,'rejected':0,'stale_skipped':0}


def db():
    con=sqlite3.connect(DB_PATH,timeout=20)
    con.row_factory=sqlite3.Row
    return con


def ensure_db():
    Path(DB_PATH).parent.mkdir(parents=True,exist_ok=True)
    with db() as con:
        con.execute('PRAGMA journal_mode=WAL')
        con.execute('PRAGMA synchronous=NORMAL')
        con.execute('PRAGMA temp_store=MEMORY')
        con.execute('PRAGMA cache_size=-12000')
        con.executescript("""
        CREATE TABLE IF NOT EXISTS rounds(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          board TEXT NOT NULL,
          external_key TEXT NOT NULL,
          result TEXT NOT NULL,
          meta_json TEXT,
          seen_at REAL NOT NULL,
          UNIQUE(board,external_key)
        );
        CREATE INDEX IF NOT EXISTS idx_rounds_board_id ON rounds(board,id);

        CREATE TABLE IF NOT EXISTS strategy_log(
          board TEXT NOT NULL,
          external_key TEXT NOT NULL,
          strategy TEXT NOT NULL,
          prediction TEXT NOT NULL,
          actual TEXT NOT NULL,
          ok INTEGER NOT NULL,
          created_at REAL NOT NULL,
          PRIMARY KEY(board,external_key,strategy)
        );
        CREATE INDEX IF NOT EXISTS idx_strategy_board_strategy ON strategy_log(board,strategy,created_at);

        CREATE TABLE IF NOT EXISTS prediction_log(
          board TEXT NOT NULL,
          external_key TEXT NOT NULL,
          prediction TEXT NOT NULL,
          confidence REAL NOT NULL,
          actual TEXT NOT NULL,
          ok INTEGER NOT NULL,
          agreement REAL,
          memory_json TEXT,
          created_at REAL NOT NULL,
          PRIMARY KEY(board,external_key)
        );
        CREATE INDEX IF NOT EXISTS idx_pred_board_time ON prediction_log(board,created_at);

        CREATE TABLE IF NOT EXISTS pattern_memory(
          board TEXT NOT NULL,
          length INTEGER NOT NULL,
          pattern TEXT NOT NULL,
          tai_count INTEGER NOT NULL DEFAULT 0,
          xiu_count INTEGER NOT NULL DEFAULT 0,
          samples INTEGER NOT NULL DEFAULT 0,
          updated_at REAL NOT NULL,
          PRIMARY KEY(board,length,pattern)
        );
        CREATE INDEX IF NOT EXISTS idx_pattern_board_samples ON pattern_memory(board,samples);

        CREATE TABLE IF NOT EXISTS auto_cau_log(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          board TEXT NOT NULL,
          external_key TEXT NOT NULL,
          strategy TEXT NOT NULL,
          prediction TEXT NOT NULL,
          actual TEXT NOT NULL,
          ok INTEGER NOT NULL,
          created_at REAL NOT NULL,
          source TEXT NOT NULL DEFAULT 'online',
          UNIQUE(board,external_key,strategy)
        );
        CREATE INDEX IF NOT EXISTS idx_auto_cau_board_id ON auto_cau_log(board,id);
        CREATE TABLE IF NOT EXISTS auto_cau_state(
          board TEXT PRIMARY KEY,
          champion TEXT,
          updated_at REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS current_prediction(
          board TEXT PRIMARY KEY,
          prediction TEXT NOT NULL,
          confidence REAL NOT NULL,
          agreement REAL NOT NULL,
          source_len INTEGER NOT NULL,
          details_json TEXT,
          updated_at REAL NOT NULL
        );
        """)
        # Non-destructive migration: old prediction rows were recreated from
        # historical rounds, not verifiable forecasts issued before settlement.
        columns={r[1] for r in con.execute('PRAGMA table_info(prediction_log)')}
        if 'source' not in columns:
            con.execute("ALTER TABLE prediction_log ADD COLUMN source TEXT NOT NULL DEFAULT 'replay'")
        con.execute("""CREATE TABLE IF NOT EXISTS issued_forecasts(
            board TEXT NOT NULL,
            base_key TEXT NOT NULL,
            prediction TEXT NOT NULL,
            created_at REAL NOT NULL,
            PRIMARY KEY(board,base_key)
        )""")
        con.executescript("""
        CREATE TABLE IF NOT EXISTS visitor_ips(
            ip TEXT PRIMARY KEY, first_seen REAL NOT NULL, last_seen REAL NOT NULL,
            requests INTEGER NOT NULL DEFAULT 0, active_seconds INTEGER NOT NULL DEFAULT 0,
            last_path TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS banned_ips(
            ip TEXT PRIMARY KEY, banned_at REAL NOT NULL, reason TEXT NOT NULL DEFAULT ''
        );
        """)
        con.executescript("""
        CREATE TABLE IF NOT EXISTS access_requests(
          token_hash TEXT PRIMARY KEY,
          request_code TEXT NOT NULL UNIQUE,
          status TEXT NOT NULL DEFAULT 'pending',
          created_at REAL NOT NULL,
          last_seen REAL NOT NULL,
          approved_at REAL,
          notified INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_access_status_created ON access_requests(status,created_at);
        """)
        con.executescript("""
        CREATE TABLE IF NOT EXISTS md5_scans(
          token_hash TEXT NOT NULL,
          hash TEXT NOT NULL,
          kind TEXT NOT NULL,
          prediction TEXT NOT NULL,
          scores_json TEXT NOT NULL,
          scanned_at REAL NOT NULL,
          PRIMARY KEY(token_hash,hash)
        );
        CREATE TABLE IF NOT EXISTS md5_labels(
          token_hash TEXT NOT NULL,
          hash TEXT NOT NULL,
          kind TEXT NOT NULL,
          actual TEXT NOT NULL,
          prior_prediction TEXT,
          scores_json TEXT NOT NULL,
          created_at REAL NOT NULL,
          PRIMARY KEY(token_hash,hash)
        );
        CREATE INDEX IF NOT EXISTS idx_md5_labels_user ON md5_labels(token_hash, created_at);
        """)
        guard.ensure_schema(con)
        con.commit()


# IP address from reverse proxy is trusted only if explicitly enabled.
TRUST_PROXY_IP = os.getenv('TRUST_PROXY_IP','0') == '1'
TRACKED_PATHS={'/','/access/status','/access/register','/config','/api/sun/current','/api/sun/history',
               '/api/bcr','/api/prediction/sunwin','/api/prediction/bcr',
               '/api/predictions/bcr','/api/learning/summary','/api/learning/boards',
               '/api/learning/top','/api/md5/analyze','/api/md5/learn','/api/md5/import'}
def visitor_ip(handler):
    raw=handler.client_address[0]
    if TRUST_PROXY_IP:
        forwarded=handler.headers.get('X-Forwarded-For','')
        if forwarded: raw=forwarded.split(',')[0].strip()
    return str(ipaddress.ip_address(raw))

def ip_action(ip,action='status',reason=''):
    ip=str(ipaddress.ip_address(ip))
    with db() as con:
        if action=='ban':
            con.execute('INSERT OR REPLACE INTO banned_ips(ip,banned_at,reason) VALUES(?,?,?)',
                        (ip,time.time(),reason[:200]))
        elif action=='unban':
            con.execute('DELETE FROM banned_ips WHERE ip=?',(ip,))
        ban=con.execute('SELECT * FROM banned_ips WHERE ip=?',(ip,)).fetchone()
        info=con.execute('SELECT * FROM visitor_ips WHERE ip=?',(ip,)).fetchone()
    return ip,dict(ban) if ban else None,dict(info) if info else None

def blocked_ip_payload(ip):
    """Return a distinct HTTP 403 state for the Netlify access screen."""
    with db() as con:
        ban=con.execute('SELECT reason FROM banned_ips WHERE ip=?',(ip,)).fetchone()
    return {'ok':False,'allowed':False,'blocked':True,'status':'ip_blocked',
            'error':'IP đã bị quản trị viên chặn','ip':ip,
            'reason':str(ban['reason'] or '')[:140] if ban else ''}

def record_visit(ip,path):
    now=time.time()
    with db() as con:
        banned=con.execute('SELECT 1 FROM banned_ips WHERE ip=?',(ip,)).fetchone()
        if banned:return False
        if path in TRACKED_PATHS:
            con.execute("""INSERT INTO visitor_ips(ip,first_seen,last_seen,requests,active_seconds,last_path)
                VALUES(?,?,?,?,?,?)
                ON CONFLICT(ip) DO UPDATE SET
                  active_seconds=visitor_ips.active_seconds+
                    CASE WHEN excluded.last_seen-visitor_ips.last_seen BETWEEN 1 AND 120
                    THEN CAST(excluded.last_seen-visitor_ips.last_seen AS INTEGER) ELSE 0 END,
                  last_seen=excluded.last_seen,requests=visitor_ips.requests+1,
                  last_path=excluded.last_path""",(ip,now,now,1,0,path))
    return True


# Access control V7: default deny. Browser creates a random token; the server
# only stores its SHA-256 hash, and the admin grants a human-readable request ID.
TOKEN_RE = re.compile(r'^[0-9a-f]{64}$')
CODE_RE = re.compile(r'^[A-F0-9]{10}$')

def access_fingerprint(token):
    if not isinstance(token,str) or not TOKEN_RE.fullmatch(token):
        return None
    return hashlib.sha256(token.encode('ascii')).hexdigest()

def access_status(token):
    fingerprint=access_fingerprint(token)
    if not fingerprint:
        return {'ok': False, 'status':'invalid', 'error':'Invalid access token'}
    with db() as con:
        r=con.execute('SELECT request_code,status FROM access_requests WHERE token_hash=?', (fingerprint,)).fetchone()
        if r:
            con.execute('UPDATE access_requests SET last_seen=? WHERE token_hash=?',(time.time(),fingerprint))
    if not r:return {'ok':True,'status':'unregistered'}
    return {'ok':True,'status':r['status'],'code':r['request_code'],'allowed':r['status']=='approved'}

def access_register(token):
    fingerprint=access_fingerprint(token)
    if not fingerprint:
        return {'ok': False, 'status':'invalid', 'error':'Invalid access token'}
    now=time.time()
    with db() as con:
        r=con.execute('SELECT request_code,status FROM access_requests WHERE token_hash=?',(fingerprint,)).fetchone()
        if r:
            con.execute('UPDATE access_requests SET last_seen=? WHERE token_hash=?',(now,fingerprint))
            return {'ok':True,'status':r['status'],'code':r['request_code'],'allowed':r['status']=='approved'}
        for _ in range(8):
            code=secrets.token_hex(5).upper()
            try:
                con.execute('INSERT INTO access_requests(token_hash,request_code,status,created_at,last_seen) VALUES(?,?,?,?,?)',
                            (fingerprint,code,'pending',now,now))
                return {'ok':True,'status':'pending','code':code,'allowed':False}
            except sqlite3.IntegrityError:
                continue
    return {'ok':False,'status':'error','error':'Cannot create request'}

def access_set(code, grant):
    code=(code or '').strip().upper()
    if not CODE_RE.fullmatch(code):
        raise ValueError('Mã yêu cầu phải gồm 10 ký tự A-F/0-9. VD: /capquyen A1B2C3D4E5')
    with db() as con:
        found=con.execute('SELECT status FROM access_requests WHERE request_code=?',(code,)).fetchone()
        if not found:raise ValueError('Không tìm thấy mã yêu cầu này.')
        status='approved' if grant else 'revoked'
        con.execute('UPDATE access_requests SET status=?,approved_at=?,notified=1 WHERE request_code=?',
                    (status,time.time() if grant else None,code))
    return f"{'✅ ĐÃ CẤP QUYỀN' if grant else '⛔ ĐÃ THU HỒI'}\nMã: <code>{code}</code>\n{'Người dùng có thể bấm Kiểm tra quyền để vào tool.' if grant else 'Người dùng bị khóa truy cập API.'}"

def access_pending_text():
    with db() as con:
        pending=con.execute("SELECT request_code,created_at FROM access_requests WHERE status='pending' ORDER BY created_at DESC LIMIT 20").fetchall()
        n=con.execute("SELECT COUNT(*) FROM access_requests WHERE status='pending'").fetchone()[0]
        approved=con.execute("SELECT COUNT(*) FROM access_requests WHERE status='approved'").fetchone()[0]
    msg=[f'<b>🔐 DUYỆT QUYỀN WEB</b>\n⏳ Chờ duyệt: <b>{n}</b> · ✅ Đã cấp: <b>{approved}</b>', '━━━━━━━━━━━━━━━━━━━━']
    if not pending:msg.append('Hiện chưa có yêu cầu chờ duyệt.')
    for r in pending:
        stamp=datetime.fromtimestamp(r['created_at'],timezone(timedelta(hours=7))).strftime('%d/%m %H:%M')
        msg.append(f'• <code>{r["request_code"]}</code> · {stamp}')
    msg.extend(['', '✅ <code>/capquyen MA</code>', '⛔ <code>/thuhoi MA</code>', '📋 <code>/yeucau</code>'])
    return '\n'.join(msg)

async def notify_access_requests(client):
    if not ADMIN_IDS:return
    with db() as con:
        waiting=[r['request_code'] for r in con.execute("SELECT request_code FROM access_requests WHERE status='pending' AND notified=0 ORDER BY created_at ASC LIMIT 12")]
    for code in waiting:
        payload={
          'text':f'<b>🔔 YÊU CẦU CẤP QUYỀN WEB</b>\nMã: <code>{code}</code>\nMặc định: ⛔ CHƯA ĐƯỢC VÀO\nAdmin chọn cấp quyền hoặc để chờ.',
          'parse_mode':'HTML',
          'reply_markup':{'inline_keyboard':[[{'text':'✅ CẤP QUYỀN','callback_data':'adm:grant:'+code},
                                              {'text':'⛔ TỪ CHỐI','callback_data':'adm:revoke:'+code}]]}
        }
        delivered=False
        for admin_id in ADMIN_IDS:
            if await tg(client,'sendMessage',{'chat_id':admin_id,**payload}) is not None:
                delivered=True
        if delivered:
            with db() as con:
                con.execute('UPDATE access_requests SET notified=1 WHERE request_code=?',(code,))

def ip_overview(limit=15):
    with db() as con:
        items=con.execute("""SELECT v.*, CASE WHEN b.ip IS NULL THEN 0 ELSE 1 END banned
          FROM visitor_ips v LEFT JOIN banned_ips b ON v.ip=b.ip
          ORDER BY v.last_seen DESC LIMIT ?""",(limit,)).fetchall()
        banned=con.execute('SELECT ip,reason FROM banned_ips ORDER BY banned_at DESC LIMIT 12').fetchall()
        total_banned=con.execute('SELECT COUNT(*) FROM banned_ips').fetchone()[0]
    lines=[f'<b>🌐 QUẢN LÝ IP · V9</b> | Đã chặn: <b>{total_banned}</b>', '━━━━━━━━━━━━━━━━━━━━']
    if not items: lines.append('Chưa ghi nhận lượt truy cập nào.')
    for r in items:
        dur=int(r['active_seconds'])
        lines.append(f'<code>{html.escape(r["ip"])}</code> · {r["requests"]} lượt'
                     f' · {dur//60}p{dur%60:02d}s'
                     f' {"⛔" if r["banned"] else "🟢"}')
    if banned:
        lines.append('\n<b>⛔ DANH SÁCH ĐANG CHẶN</b>')
        for r in banned:
            lines.append(f'• <code>{html.escape(r["ip"])}</code>'
                         + (f' · {html.escape(str(r["reason"])[:45])}' if r['reason'] else ''))
    if not TRUST_PROXY_IP:
        lines.append('⚠️ <i>TRUST_PROXY_IP=0: có thể đang ghi IP của proxy Railway; kiểm tra kỹ trước khi chặn.</i>')
    lines.extend(['\n<b>Chặn/mở nhanh:</b> dùng nút ở dưới.',
                  '<code>/banip 1.2.3.4</code> · <code>/unbanip 1.2.3.4</code>',
                  '<i>Thời gian hoạt động chỉ ước tính từ các lượt truy cập.</i>'])
    return '\n'.join(lines)

def ip_detail(ip):
    ip,ban,r=ip_action(ip)
    if not r:return f'<code>{html.escape(ip)}</code> · '+('⛔ Đã chặn, chưa ghi nhận lượt truy cập.' if ban else 'Chưa ghi nhận truy cập.')
    def when(v):return datetime.fromtimestamp(v,timezone(timedelta(hours=7))).strftime('%d/%m %H:%M:%S')
    return ('<b>🔎 CHI TIẾT IP</b>\n'
            f'<code>{html.escape(ip)}</code>\n'
            f'Trạng thái: {"⛔ Đã chặn" if ban else "🟢 Được phép"}\n'
            f'Lần đầu: {when(r["first_seen"])}\n'
            f'Lần cuối: {when(r["last_seen"])}\n'
            f'Số lượt: {r["requests"]}\n'
            f'Hoạt động ước tính: {r["active_seconds"]//60} phút\\n'
            f'Đường dẫn cuối: <code>{html.escape(r["last_path"])}</code>')

def safe_url(v):
    v=(v or '').strip()
    p=urlparse(v)
    if p.scheme not in ('http','https') or not p.netloc:
        raise ValueError('URL phải bắt đầu bằng http:// hoặc https://')
    return v


def load_config():
    global CONFIG
    data=dict(DEFAULT_CONFIG)
    try:
        if CONFIG_PATH.exists():
            obj=json.loads(CONFIG_PATH.read_text(encoding='utf-8'))
            if isinstance(obj,dict): data.update({k:v for k,v in obj.items() if k in data})
    except Exception: pass
    with LOCK: CONFIG=data


def save_config():
    CONFIG_PATH.parent.mkdir(parents=True,exist_ok=True)
    with LOCK:
        CONFIG['updated_at']=time.time()
        tmp=CONFIG_PATH.with_suffix('.tmp')
        tmp.write_text(json.dumps(CONFIG,ensure_ascii=False,indent=2),encoding='utf-8')
        tmp.replace(CONFIG_PATH)


def public_config():
    with LOCK: return dict(CONFIG)


def cache_public(name):
    with LOCK: return dict(CACHE.get(name) or {})


def json_bytes(obj):
    return json.dumps(obj,ensure_ascii=False,separators=(',',':')).encode('utf-8')


def unwrap(x):
    for _ in range(5):
        if isinstance(x,dict) and 'data' in x:
            x=x.get('data')
        else: break
    return x


def pick(o,keys):
    if not isinstance(o,dict): return None
    for k in keys:
        if k in o and o[k] is not None: return o[k]
    return None


def _ascii(v):
    s=str(v or '').upper()
    tr=str.maketrans('ÀÁẢÃẠÂẦẤẨẪẬĂẰẮẲẴẶÈÉẺẼẸÊỀẾỂỄỆÌÍỈĨỊÒÓỎÕỌÔỒỐỔỖỘƠỜỚỞỠỢÙÚỦŨỤƯỪỨỬỮỰỲÝỶỸỴĐ',
                     'AAAAAAAAAAAAAAAAAEEEEEEEEEEEIIIIIOOOOOOOOOOOOOOOOOUUUUUUUUUUUYYYYYD')
    return s.translate(tr)


def sun_side(v,total=None):
    s=_ascii(v)
    if 'TAI' in s or s in ('T','BIG'): return 'TÀI'
    if 'XIU' in s or s in ('X','SMALL'): return 'XỈU'
    try:
        n=float(total)
        if 3<=n<=18: return 'TÀI' if n>=11 else 'XỈU'
    except Exception: pass
    return None


def sun_item(o,i=0):
    if not isinstance(o,dict): return None
    d1=pick(o,['d1','dice1','xuc_xac_1','xx1','x1'])
    d2=pick(o,['d2','dice2','xuc_xac_2','xx2','x2'])
    d3=pick(o,['d3','dice3','xuc_xac_3','xx3','x3'])
    dice=[]
    for x in (d1,d2,d3):
        try: dice.append(int(x))
        except Exception: pass
    total=pick(o,['total','tong','sum','score'])
    try: total=int(total)
    except Exception:
        total=sum(dice) if len(dice)==3 else None
    result=sun_side(pick(o,['result','ket_qua','ketqua','tai_xiu','tx','outcome','gameResult']),total)
    sid=pick(o,['phien','session','session_id','id','round','roundId','issue','gameId'])
    # A position in a fetched list is not a stable session ID.
    if sid is None or not str(sid).strip() or not result: return None
    return {'key':str(sid),'result':result,'meta':{'sid':str(sid),'dice':dice,'total':total}}


def normalize_sun(payload):
    x=unwrap(payload)
    arr=[]
    if isinstance(x,list): arr=x
    elif isinstance(x,dict):
        for k in ('history','results','list','items','rows'):
            if isinstance(x.get(k),list): arr=x[k];break
        if not arr: arr=[v for v in x.values() if isinstance(v,dict)]
    out=[]
    for i,o in enumerate(arr):
        z=sun_item(o,i)
        if z: out.append(z)
    # Keep duplicate IDs so the consistency guard can detect conflicts.
    def sk(z):
        m=re.findall(r'\d+',z['key'])
        return (int(m[-1]) if m else 0,z['key'])
    out.sort(key=sk)
    return out[-MAX_BACKFILL_PER_BOARD:]


def normalize_sun_current(payload):
    x=unwrap(payload)
    if isinstance(x,list): x=x[-1] if x else None
    z=sun_item(x or {},0)
    return [z] if z else []


def normalize_bcr(payload):
    x=unwrap(payload)
    if isinstance(x,dict):
        arr=x.get('items') or x.get('tables') or x.get('results') or []
    else: arr=x if isinstance(x,list) else []
    boards={}
    for i,it in enumerate(arr):
        if not isinstance(it,dict): continue
        table=str(it.get('table') or it.get('ban') or it.get('name') or it.get('tableName') or f'Bàn {i+1}').strip()
        results=str(it.get('result') or it.get('results') or it.get('history') or it.get('cau') or '').upper()
        results=''.join(c for c in results if c in 'BPT')
        if not results: continue
        shoe=str(it.get('shoeId') or it.get('shoe_id') or it.get('shoe') or '')
        tail=results[-MAX_BACKFILL_PER_BOARD:]
        offset=max(0,len(results)-len(tail))
        rows=[]
        for j,c in enumerate(tail):
            if c=='T': continue
            prefix=results[:offset+j+1]
            digest=hashlib.sha1((table+'|'+shoe+'|'+prefix).encode()).hexdigest()[:12]
            key=f'{shoe or "shoe"}:{offset+j+1}:{digest}'
            rows.append({'key':key,'result':'TÀI' if c=='B' else 'XỈU',
                         'meta':{'table':table,'shoe':shoe,'pos':offset+j+1,'bcr':c,
                                 'good_road':it.get('good_road') or it.get('goodRoad') or it.get('road') or ''}})
        boards['baccarat:'+table]=rows
    return boards


def load_seq(con,board,limit=1400):
    order='CAST(external_key AS INTEGER) DESC,id DESC' if board.startswith('sunwin:') else 'id DESC'
    rows=con.execute(f'SELECT result FROM rounds WHERE board=? ORDER BY {order} LIMIT ?',(board,limit)).fetchall()
    return [r['result'] for r in reversed(rows)]


def load_history_context(con,board,limit=1400):
    order='CAST(external_key AS INTEGER) DESC,id DESC' if board.startswith('sunwin:') else 'id DESC'
    rows=con.execute(f'SELECT external_key,result,meta_json FROM rounds WHERE board=? ORDER BY {order} LIMIT ?',(board,limit)).fetchall()
    records=[]
    for r in reversed(rows):
        try:meta=json.loads(r['meta_json'] or '{}')
        except Exception:meta={}
        records.append((str(r['external_key']),r['result'],meta))
    segment=acl.trailing_segment(records,board)
    return [r[1] for r in segment],[r[2] for r in segment]


def bootstrap_auto_cau_history(con,board):
    """Populate expert scores causally from existing rounds after upgrade.

    Only considers result prefixes available before each historical outcome.
    Retrospective walk-forward logs are tagged and must not be confused with
    live observations; does not overwrite historical predictions or TXT.
    """
    # V6 is a new catalog with changed algorithms. Score the V6 experts
    # from historical prefixes even if V4/V5 logs already exist. Otherwise a
    # deployed V5 database incorrectly prevents the new models from learning.
    have=con.execute("SELECT 1 FROM auto_cau_log WHERE board=? AND strategy LIKE 'V6_%' LIMIT 1",(board,)).fetchone()
    if have:return 0
    order='CAST(r.external_key AS INTEGER) ASC,r.id ASC' if board.startswith('sunwin:') else 'r.id ASC'
    old=con.execute(f'''SELECT r.external_key,r.result,r.meta_json,r.seen_at,p.prediction legacy
          FROM rounds r LEFT JOIN prediction_log p
          ON p.board=r.board AND p.external_key=r.external_key
          WHERE r.board=? ORDER BY {order}''',(board,)).fetchall()
    if len(old)<13:return 0
    history=[];metas=[];last_key=None;last_meta={};count=0
    for r in old:
        key=str(r['external_key']);actual=r['result']
        try:meta=json.loads(r['meta_json'] or '{}')
        except Exception:meta={}
        if last_key is not None and not acl.id_contiguous(last_key,key,last_meta,meta,board):
            history=[];metas=[]
        if len(history)>=12 and actual in ('TÀI','XỈU'):
            preds=acl.strategy_predictions(history,board,metas,r['legacy'])
            con.executemany('INSERT OR IGNORE INTO auto_cau_log(board,external_key,strategy,prediction,actual,ok,created_at,source) VALUES(?,?,?,?,?,?,?,?)',
                [(board,key,name,pred,actual,1 if pred==actual else 0,float(r['seen_at'] or 0),'backfill') for name,pred in preds.items()])
            count+=1
        history.append(actual);metas.append(meta)
        last_key=key;last_meta=meta
    return count


def load_auto_cau_logs(con,board):
    from collections import deque
    # Read the latest 100 settled observations per V6 expert, retaining the
    # actual/predicted labels so each candidate is compared with a fixed-side
    # baseline on THE SAME rounds (not an unrelated recent class ratio).
    rows=con.execute('''SELECT strategy,ok,actual,prediction FROM auto_cau_log
        WHERE board=? AND strategy LIKE 'V6_%' ORDER BY id DESC LIMIT 8000''',(board,)).fetchall()
    logs={}
    for r in reversed(rows):
        logs.setdefault(r['strategy'],deque(maxlen=100)).append({
          'correct':int(r['ok']), 'actual':r['actual'], 'prediction':r['prediction']})
    return logs


def load_auto_champion(con,board):
    row=con.execute('SELECT champion FROM auto_cau_state WHERE board=?',(board,)).fetchone()
    return str(row['champion']) if row and row['champion'] else None


def save_auto_champion(con,board,champion,ts):
    if not champion:return
    con.execute('''INSERT INTO auto_cau_state(board,champion,updated_at) VALUES(?,?,?)
        ON CONFLICT(board) DO UPDATE SET champion=excluded.champion,updated_at=excluded.updated_at''',(board,champion,ts))


def perf_map(con,board):
    # All-time + recent + class-balanced stats. Recent window reacts to regime changes;
    # class stats reduce one-sided strategies from dominating on imbalanced runs.
    rows=con.execute("""
        SELECT strategy,
               COUNT(*) n,
               SUM(ok) wins,
               SUM(CASE WHEN actual='TÀI' THEN 1 ELSE 0 END) tai_n,
               SUM(CASE WHEN actual='TÀI' AND ok=1 THEN 1 ELSE 0 END) tai_wins,
               SUM(CASE WHEN actual='XỈU' THEN 1 ELSE 0 END) xiu_n,
               SUM(CASE WHEN actual='XỈU' AND ok=1 THEN 1 ELSE 0 END) xiu_wins
        FROM strategy_log WHERE board=? GROUP BY strategy
    """,(board,)).fetchall()
    out={r['strategy']:{
        'n':int(r['n'] or 0),'wins':int(r['wins'] or 0),
        'tai_n':int(r['tai_n'] or 0),'tai_wins':int(r['tai_wins'] or 0),
        'xiu_n':int(r['xiu_n'] or 0),'xiu_wins':int(r['xiu_wins'] or 0),
        'recent_n':0,'recent_wins':0,
    } for r in rows}
    recent=con.execute("""
      WITH ranked AS (
        SELECT strategy,ok,
               ROW_NUMBER() OVER(PARTITION BY strategy ORDER BY created_at DESC) AS rn
        FROM strategy_log WHERE board=?
      )
      SELECT strategy,COUNT(*) recent_n,SUM(ok) recent_wins
      FROM ranked WHERE rn<=160 GROUP BY strategy
    """,(board,)).fetchall()
    for r in recent:
        st=out.setdefault(r['strategy'],{'n':0,'wins':0,'tai_n':0,'tai_wins':0,'xiu_n':0,'xiu_wins':0})
        st['recent_n']=int(r['recent_n'] or 0);st['recent_wins']=int(r['recent_wins'] or 0)
    return out


def memory_signal(con,board,seq):
    best=None
    for k in range(min(12,len(seq)),1,-1):
        pat=''.join('T' if x=='TÀI' else 'X' for x in seq[-k:])
        r=con.execute('SELECT tai_count,xiu_count,samples FROM pattern_memory WHERE board=? AND length=? AND pattern=?',(board,k,pat)).fetchone()
        if not r: continue
        n=int(r['samples'] or 0)
        if n<3: continue
        p=(int(r['tai_count'])+2)/(n+4)
        edge=abs(p-.5)
        quality=edge*min(1,n/18)
        if best is None or quality>best['quality']:
            best={'pattern':pat,'length':k,'support':n,'p_tai':p,'quality':quality}
    return best


def update_pattern_memory(con,board,seq,actual,ts):
    for k in range(2,min(12,len(seq))+1):
        pat=''.join('T' if x=='TÀI' else 'X' for x in seq[-k:])
        t=1 if actual=='TÀI' else 0;x=1-t
        con.execute('''
          INSERT INTO pattern_memory(board,length,pattern,tai_count,xiu_count,samples,updated_at)
          VALUES(?,?,?,?,?,?,?)
          ON CONFLICT(board,length,pattern) DO UPDATE SET
            tai_count=tai_count+excluded.tai_count,
            xiu_count=xiu_count+excluded.xiu_count,
            samples=samples+1,
            updated_at=excluded.updated_at
        ''',(board,k,pat,t,x,1,ts))


def create_current_prediction(con,board,seq,meta_history=None):
    if not seq: return None
    perf=perf_map(con,board);mem=memory_signal(con,board,seq)
    e=ensemble_prediction(seq,perf=perf,memory=mem,board=board,meta_history=meta_history)
    caulog=load_auto_cau_logs(con,board)
    caupred=acl.strategy_predictions(seq,board,meta_history,e['prediction'])
    caudecision=acl.select_predict(caupred,caulog,load_auto_champion(con,board),seq)
    cauinfo=acl.detect_cau(seq)
    final=caudecision['prediction']
    # Keep calibration conservative: retrospective strategy selection is not a
    # verified probability of a fair, independently random outcome.
    conf=min(55.0, 50.0+max(0.0,caudecision['historical_score']-.50)*35.0) if caudecision['champion'] else 50.0
    audit=gate_live(con,board,len(seq),bool(caudecision.get('champion')))
    d={'board':board,'prediction':final,'confidence':conf,
       'agreement':round(e['agreement']*100,1),'source_len':len(seq),
       'strategy_count':e.get('strategy_count',len(STRATEGY_NAMES)),'memory':e.get('memory'),
       'engine':e.get('engine','OMNI-MAX'),'regime':e.get('regime','MIXED'),'edge':e.get('edge',0),
       'top':e.get('top',[])[:8],'updated_at':time.time(),
       'cau_type':cauinfo['type'],'cau_clarity':cauinfo['clarity'],
       'cau_run_length':cauinfo['run_length'],'cau_mode':caudecision['mode'],
       'cau_champion':caudecision['champion'],
       'cau_samples':caudecision['sample_count'],
       'cau_baseline':caudecision.get('baseline_score',.5),
       'cau_top':caudecision['top'],
       'audit_signal_code':audit.get('signal_code'),
       'audit_signal_reason':audit.get('signal_reason'),
       'live_stats':audit.get('live_stats',{}),
       'signal_is_win_probability':False,
       'signal_status':'predict'}
    # Expose the settled base round used for the NEXT forecast. This enables
    # clients to detect stale/mismatched current-round data before display.
    ordering='CAST(external_key AS INTEGER) DESC,id DESC' if board.startswith('sunwin:') else 'id DESC'
    used=con.execute(f'SELECT external_key FROM rounds WHERE board=? ORDER BY {ordering} LIMIT 1',(board,)).fetchone()
    d['base_key']=str(used['external_key']) if used else None
    con.execute('''
      INSERT INTO current_prediction(board,prediction,confidence,agreement,source_len,details_json,updated_at)
      VALUES(?,?,?,?,?,?,?)
      ON CONFLICT(board) DO UPDATE SET prediction=excluded.prediction,confidence=excluded.confidence,
        agreement=excluded.agreement,source_len=excluded.source_len,details_json=excluded.details_json,
        updated_at=excluded.updated_at
    ''',(board,d['prediction'],d['confidence'],d['agreement'],len(seq),json.dumps(d,ensure_ascii=False,separators=(',',':')),d['updated_at']))
    # Immutable pre-outcome forecast for auditing true live accuracy. This
    # records what the web/API would show *before* the next round is received.
    # A reboot does not overwrite the first forecast for a given base round.
    order='CAST(external_key AS INTEGER) DESC,id DESC' if board.startswith('sunwin:') else 'id DESC'
    base=con.execute(f'SELECT external_key FROM rounds WHERE board=? ORDER BY {order} LIMIT 1',(board,)).fetchone()
    if base:
        con.execute('INSERT OR IGNORE INTO issued_forecasts(board,base_key,prediction,created_at) VALUES(?,?,?,?)',
                    (board,str(base['external_key']),final,d['updated_at']))
    with LOCK: LATEST[board]=d
    return d


def learn_rows(board,rows,source='unspecified'):
    if not rows: return 0
    now=time.time();new_count=0
    with db() as con:
        seq,meta_history=load_history_context(con,board)
        bootstrap_auto_cau_history(con,board)
        order='CAST(external_key AS INTEGER) DESC,id DESC' if board.startswith('sunwin:') else 'id DESC'
        latest_row=con.execute(f'SELECT external_key FROM rounds WHERE board=? ORDER BY {order} LIMIT 1',(board,)).fetchone()
        rows, guard_stats = guard.prepare(con,board,rows,source,now,str(latest_row['external_key']) if latest_row else None)
        with LOCK:
            LEARN_STATUS['rejected']+=guard_stats['rejected']
            LEARN_STATUS['stale_skipped']+=guard_stats['stale']
        existing={r['external_key'] for r in con.execute('SELECT external_key FROM rounds WHERE board=?',(board,))}
        perf=perf_map(con,board)
        auto_logs=load_auto_cau_logs(con,board)
        auto_champion=load_auto_champion(con,board)
        last_row=con.execute(f'SELECT external_key,meta_json FROM rounds WHERE board=? ORDER BY {order} LIMIT 1',(board,)).fetchone()
        last_key=str(last_row['external_key']) if last_row else None
        try:last_meta=json.loads(last_row['meta_json'] or '{}') if last_row else {}
        except Exception:last_meta={}
        pending=[item for item in rows if item.get('result') in ('TÀI','XỈU') and str(item.get('key')) not in existing]
        cold_skip=max(0,len(pending)-72) if not seq else 0
        pending_i=0
        for item in rows:
            key=str(item.get('key'));actual=item.get('result')
            if actual not in ('TÀI','XỈU') or key in existing: continue
            cur_meta=item.get('meta') or {}
            if last_key is not None and not acl.id_contiguous(last_key,key,last_meta,cur_meta,board):
                # A gap or new Baccarat shoe ends the previous curve. Never
                # treat distant rounds as one contiguous transition.
                seq=[];meta_history=[]
            do_eval=(pending_i>=cold_skip)
            pending_i+=1
            if len(seq)>=12 and do_eval:
                mem=memory_signal(con,board,seq)
                main=ensemble_prediction(seq,perf=perf,memory=mem,board=board,meta_history=meta_history)
                auto_preds=acl.strategy_predictions(seq,board,meta_history,main['prediction'])
                decision=acl.select_predict(auto_preds,auto_logs,auto_champion,seq)
                if decision['champion']:
                    auto_champion=decision['champion']
                    save_auto_champion(con,board,auto_champion,now)
                # Every expert predicts BEFORE this round's actual is known.
                con.executemany('INSERT OR IGNORE INTO auto_cau_log(board,external_key,strategy,prediction,actual,ok,created_at) VALUES(?,?,?,?,?,?,?)',[(board,key,name,pred,actual,1 if pred==actual else 0,now+pending_i*.00001)
                    for name,pred in auto_preds.items()])
                acl.record_settled(auto_logs,auto_preds,actual)
                final_pred=decision['prediction']
                # Only count a forecast as LIVE if it was stored before the
                # result and directly followed the previous observed round.
                issued=None
                if last_key is not None and acl.id_contiguous(last_key,key,last_meta,cur_meta,board):
                    issued=con.execute('SELECT prediction,created_at FROM issued_forecasts WHERE board=? AND base_key=?',
                                       (board,last_key)).fetchone()
                is_live=bool(issued and issued['created_at']<now and issued['prediction'] in ('TÀI','XỈU'))
                if is_live: final_pred=issued['prediction']
                preds=main.get('strategies') or strategy_predictions(seq,board,meta_history)
                logs=[]
                for name,pred in preds.items():
                    ok=1 if pred==actual else 0
                    logs.append((board,key,name,pred,actual,ok,now))
                    st=perf.setdefault(name,{'n':0,'wins':0});st['n']+=1;st['wins']+=ok
                if logs:
                    con.executemany('INSERT OR IGNORE INTO strategy_log(board,external_key,strategy,prediction,actual,ok,created_at) VALUES(?,?,?,?,?,?,?)',logs)
                con.execute('INSERT OR IGNORE INTO prediction_log(board,external_key,prediction,confidence,actual,ok,agreement,memory_json,created_at,source) VALUES(?,?,?,?,?,?,?,?,?,?)',
                            (board,key,final_pred,min(55.,50.+max(0.,decision['historical_score']-.50)*35.) if decision['champion'] else 50.0,actual,1 if final_pred==actual else 0,float(main.get('agreement',.5))*100,
                             json.dumps(main.get('memory'),ensure_ascii=False,separators=(',',':')) if main.get('memory') else None,now,
                             'live' if is_live else 'replay'))
                # Invariants: only newly settled, genuinely pre-outcome LIVE
                # forecasts issued in this deployment advance the 30-round gate.
                if is_live:
                    warmup.mark_forward_live(con,board,key,issued['created_at'],now)
            update_pattern_memory(con,board,seq,actual,now)
            con.execute('INSERT INTO rounds(board,external_key,result,meta_json,seen_at) VALUES(?,?,?,?,?)',(board,key,actual,json.dumps(item.get('meta') or {},ensure_ascii=False,separators=(',',':')),now))
            existing.add(key);seq.append(actual);meta_history.append(cur_meta);new_count+=1
            last_key=key;last_meta=cur_meta
        create_current_prediction(con,board,seq,meta_history);con.commit()
    return new_count


def init_deploy_warmup():
    """Initialize persistent display gating for this exact deployment."""
    with db() as con:
        warmup.ensure_schema(con)


def warmup_for(board):
    with db() as con:
        return warmup.progress(con,board)


def upgrade_existing_history():
    """One-time causal replay of prior saved rounds on first new deployment.

    Does not alter pre-existing prediction_log or TXT records.
    """
    with db() as con:
        boards=[r[0] for r in con.execute('SELECT DISTINCT board FROM rounds')]
        for board in boards:
            bootstrap_auto_cau_history(con,board)
            seq,metas=load_history_context(con,board)
            if seq:create_current_prediction(con,board,seq,metas)
        con.commit()


def learn_payloads(sun_history=None,sun_current=None,bcr_payload=None):
    total=0
    try:
        if sun_history is not None: total+=learn_rows('sunwin:hu',normalize_sun(sun_history),'sun_history')
        if sun_current is not None: total+=learn_rows('sunwin:hu',normalize_sun_current(sun_current),'sun_current')
        if bcr_payload is not None:
            for board,rows in normalize_bcr(bcr_payload).items(): total+=learn_rows(board,rows,'bcr_api')
        with LOCK:
            LEARN_STATUS['last_cycle']=time.time();LEARN_STATUS['new_rounds']+=total;LEARN_STATUS['cycles']+=1;LEARN_STATUS['last_error']=''
            if total>0: LEARN_STATUS['last_new_round_at']=time.time()
    except Exception as e:
        with LOCK: LEARN_STATUS['last_cycle']=time.time();LEARN_STATUS['last_error']=str(e)[:240]
    return total


def load_latest_from_db():
    with db() as con:
        for r in con.execute('SELECT board,details_json FROM current_prediction'):
            try:d=json.loads(r['details_json'])
            except Exception:continue
            LATEST[r['board']]=d


def summary_payload():
    with db() as con:
        rounds=int(con.execute('SELECT COUNT(*) FROM rounds').fetchone()[0]);boards=int(con.execute('SELECT COUNT(DISTINCT board) FROM rounds').fetchone()[0])
        patterns=int(con.execute('SELECT COUNT(*) FROM pattern_memory').fetchone()[0]);mature=int(con.execute('SELECT COUNT(*) FROM pattern_memory WHERE samples>=5').fetchone()[0])
        sample=int(con.execute('SELECT COALESCE(SUM(samples),0) FROM pattern_memory').fetchone()[0]);pr=con.execute('SELECT COUNT(*) n,COALESCE(SUM(ok),0) w FROM prediction_log').fetchone()
        sn=int(pr['n'] or 0);sw=int(pr['w'] or 0);strategies=int(con.execute('SELECT COUNT(DISTINCT strategy) FROM strategy_log').fetchone()[0])
        lv=con.execute("SELECT COUNT(*) n,COALESCE(SUM(ok),0) w FROM prediction_log WHERE source='live'").fetchone()
        ln=int(lv['n'] or 0);lw=int(lv['w'] or 0)
    with db() as con:
        ac=con.execute("SELECT COUNT(*) n,COUNT(DISTINCT strategy) strategies,COALESCE(SUM(CASE WHEN source='backfill' THEN 1 ELSE 0 END),0) backfill FROM auto_cau_log").fetchone()
    with LOCK:st=dict(LEARN_STATUS)
    return {'ok':True,'auto_cau_logs':int(ac['n'] or 0),'auto_cau_strategies':int(ac['strategies'] or 0),'auto_cau_backfill':int(ac['backfill'] or 0),'rounds':rounds,'boards':boards,'patterns':patterns,'mature_patterns':mature,'pattern_samples':sample,
            'settled_predictions':sn,'wins':sw,'win_rate':round(sw/sn*100,2) if sn else None,
            'live_predictions':ln,'live_wins':lw,'live_win_rate':round(lw/ln*100,2) if ln else None,
            'replay_predictions':sn-ln,'strategies':strategies,
            'engine':f'CLASSIC V3 + AUTO CẦU ({len(STRATEGY_NAMES)} chiến lược gốc)','learner':st}


def board_rows_payload():
    with db() as con:
        rows=con.execute('''
          SELECT r.board,COUNT(*) rounds,
                 (SELECT COUNT(*) FROM pattern_memory p WHERE p.board=r.board) patterns,
                 (SELECT COUNT(*) FROM pattern_memory p WHERE p.board=r.board AND p.samples>=5) mature,
                 (SELECT COUNT(*) FROM prediction_log q WHERE q.board=r.board) pn,
                 (SELECT COALESCE(SUM(ok),0) FROM prediction_log q WHERE q.board=r.board) pw
          FROM rounds r GROUP BY r.board ORDER BY rounds DESC
        ''').fetchall()
    out=[]
    for r in rows:
        n=int(r['pn'] or 0);w=int(r['pw'] or 0)
        out.append({'board':r['board'],'rounds':int(r['rounds']),'patterns':int(r['patterns'] or 0),'mature':int(r['mature'] or 0),
                    'predictions':n,'wins':w,'win_rate':round(w/n*100,1) if n else None})
    return out


def top_algo_payload(board=None,limit=20):
    where='WHERE board=?' if board else '';params=[board] if board else []
    with db() as con:
        rows=con.execute(f'''SELECT strategy,COUNT(*) n,SUM(ok) wins FROM strategy_log {where}
                            GROUP BY strategy HAVING COUNT(*)>=8
                            ORDER BY (SUM(ok)+8.0)/(COUNT(*)+16.0) DESC,COUNT(*) DESC LIMIT ?''',params+[int(limit)]).fetchall()
    return [{'strategy':r['strategy'],'n':int(r['n']),'wins':int(r['wins']),'rate':round(int(r['wins'])/int(r['n'])*100,1),
             'bayes':round((int(r['wins'])+8)/(int(r['n'])+16)*100,1)} for r in rows]


def prediction_payload(board):
    # Never stop the backend forecasts. Only hide the public prediction during
    # the first 30 settled LIVE forecasts of THIS deployment and board.
    gate=warmup_for(board)
    with LOCK:d=dict(LATEST.get(board) or {})
    if not d:
        with db() as con:r=con.execute('SELECT details_json FROM current_prediction WHERE board=?',(board,)).fetchone()
        if r:
            try:d=json.loads(r['details_json'])
            except Exception:d={}
    if not d:
        return {'ok':True,'board':board,'prediction':None,'display':'ĐANG ĐỒNG BỘ',
                'prediction_bcr':None,'class':None,'signal_status':'warming',
                'signal_reason':f'Đang thu thập dự đoán LIVE {gate["warmup_count"]}/{gate["warmup_target"]}',
                **gate}
    # Keep stale predictions visibly labeled, but never pretend cached values
    # refer to a new live round. Cached forecast remains available post-warmup.
    max_age=180 if board.startswith('sunwin:') else 900
    age=time.time()-float(d.get('updated_at') or 0)
    stale=age>max_age
    out={'ok':True,**d,**gate,'stale':stale,
         'signal_status':'predict' if gate['warmup_ready'] else 'warming',
         'signal_reason':('Dữ liệu đã cũ, kiểm tra API trước khi sử dụng' if stale else
                          'Đang chạy ngầm, chờ 30 dự đoán LIVE hoàn tất' if not gate['warmup_ready'] else
                          'Đang dự đoán liên tục; điểm không phải xác suất thắng')}
    if not gate['warmup_ready']:
        # Do not leak hidden prediction through side-specific fields, details
        # or top strategies while the 30-round display gate is closed.
        for key in ('prediction','display','prediction_bcr','class','top','cau_top'):
            out.pop(key,None)
        out.update({'prediction':None,'display':f'KHỞI ĐỘNG {gate["warmup_count"]}/{gate["warmup_target"]}',
                    'prediction_bcr':None,'class':None})
        return out
    if board.startswith('baccarat:'):
        out['prediction_bcr']='Banker' if d.get('prediction')=='TÀI' else 'Player'
        out['class']='B' if d.get('prediction')=='TÀI' else 'P'
    else: out['display']='TÀI' if d.get('prediction')=='TÀI' else 'XỈU'
    return out



# MD5/SHA256 remains server-side: the HTML only transports input and displays
# results. Every record is isolated to an approved browser token fingerprint.
MD5_RATE_LOCK=threading.Lock()
MD5_RATE={}

def md5_rate_allowed(token_hash, limit=30):
    now=time.monotonic()
    with MD5_RATE_LOCK:
        if len(MD5_RATE)>10000:
            for k in list(MD5_RATE):
                if not MD5_RATE[k] or MD5_RATE[k][-1]<now-60: MD5_RATE.pop(k,None)
        xs=[x for x in MD5_RATE.get(token_hash,[]) if now-x<60]
        if len(xs)>=limit:
            MD5_RATE[token_hash]=xs
            return False
        xs.append(now);MD5_RATE[token_hash]=xs
        return True

def md5_labels_for(con,token_hash):
    rows=con.execute('SELECT hash,kind,actual,scores_json FROM md5_labels '
                     'WHERE token_hash=? ORDER BY created_at DESC LIMIT 1500',(token_hash,)).fetchall()
    return [{'hash':r['hash'],'kind':r['kind'],'actual':r['actual'],
             'scores':json.loads(r['scores_json'])} for r in rows]

def md5_analyze(token_hash,code):
    code=str(code or '').strip().lower()
    if not HASH_RE.fullmatch(code):
        raise ValueError('Chỉ hỗ trợ MD5 32 HEX hoặc SHA256 64 HEX')
    with db() as con:
        labels=md5_labels_for(con,token_hash)
        result=weighted_result(code,labels)
        scores=scores288(code)
        con.execute('INSERT INTO md5_scans(token_hash,hash,kind,prediction,scores_json,scanned_at) '
                    'VALUES(?,?,?,?,?,?) ON CONFLICT(token_hash,hash) DO UPDATE SET '
                    'prediction=excluded.prediction,scores_json=excluded.scores_json,scanned_at=excluded.scanned_at',
                    (token_hash,code,result['kind'],result['prediction'],json.dumps(scores),time.time()))
        con.execute('DELETE FROM md5_scans WHERE token_hash=? AND scanned_at < ?',
                    (token_hash,time.time()-14*86400))
    return result

def md5_learn(token_hash,code,actual):
    code=str(code or '').strip().lower()
    if not HASH_RE.fullmatch(code):raise ValueError('Hash không hợp lệ')
    if actual not in ('TÀI','XỈU'):raise ValueError('Kết quả chỉ được là TÀI hoặc XỈU')
    with db() as con:
        old=con.execute('SELECT 1 FROM md5_labels WHERE token_hash=? AND hash=?',(token_hash,code)).fetchone()
        if old:raise ValueError('Hash này đã được ghi kết quả')
        scan=con.execute('SELECT * FROM md5_scans WHERE token_hash=? AND hash=?',(token_hash,code)).fetchone()
        if scan is None:raise ValueError('Phải phân tích hash trước khi ghi kết quả')
        con.execute('INSERT INTO md5_labels(token_hash,hash,kind,actual,prior_prediction,scores_json,created_at) '
                    'VALUES(?,?,?,?,?,?,?)',
                    (token_hash,code,scan['kind'],actual,scan['prediction'],scan['scores_json'],time.time()))
        con.execute('DELETE FROM md5_scans WHERE token_hash=? AND hash=?',(token_hash,code))
        con.execute('DELETE FROM md5_labels WHERE token_hash=? AND hash IN '
                    '(SELECT hash FROM md5_labels WHERE token_hash=? ORDER BY created_at DESC LIMIT -1 OFFSET 1500)',
                    (token_hash,token_hash))
        total=con.execute('SELECT COUNT(*) FROM md5_labels WHERE token_hash=?',(token_hash,)).fetchone()[0]
    return {'ok':True,'message':'Đã ghi kết quả lên Railway','kind':scan['kind'],'actual':actual,'learned':total}

def md5_import(token_hash,rows):
    if not isinstance(rows,list) or len(rows)>40:raise ValueError('Tối đa 40 bản ghi mỗi lần')
    valid=[];seen=set()
    for row in rows:
        if not isinstance(row,dict):continue
        code=str(row.get('hash') or '').strip().lower()
        actual=row.get('actual')
        if not HASH_RE.fullmatch(code) or actual not in ('TÀI','XỈU') or code in seen:continue
        valid.append((code,actual));seen.add(code)
    count=0
    with db() as con:
        for code,actual in valid:
            # Skip existing without repeating expensive feature extraction.
            if con.execute('SELECT 1 FROM md5_labels WHERE token_hash=? AND hash=?',(token_hash,code)).fetchone():continue
            kind='MD5' if len(code)==32 else 'SHA256'
            scores=scores288(code)
            cur=con.execute('INSERT OR IGNORE INTO md5_labels(token_hash,hash,kind,actual,prior_prediction,scores_json,created_at) '
                            'VALUES(?,?,?,?,?,?,?)',
                            (token_hash,code,kind,actual,None,json.dumps(scores),time.time()))
            count+=cur.rowcount
        con.execute('DELETE FROM md5_labels WHERE token_hash=? AND hash IN '
                    '(SELECT hash FROM md5_labels WHERE token_hash=? ORDER BY created_at DESC LIMIT -1 OFFSET 1500)',
                    (token_hash,token_hash))
    return {'ok':True,'imported':count}

class Handler(BaseHTTPRequestHandler):
    server_version='ThanhtungLearning/4.0-OMNIMAX'
    def log_message(self,fmt,*args): return
    def send_bytes(self,code,body,ctype='application/json; charset=utf-8'):
        self.send_response(code);self.send_header('Content-Type',ctype);self.send_header('Content-Length',str(len(body)))
        self.send_header('Cache-Control','no-store, no-cache, must-revalidate');self.send_header('Access-Control-Allow-Origin','*')
        self.send_header('Access-Control-Allow-Methods','GET, POST, OPTIONS');self.send_header('Access-Control-Allow-Headers','Content-Type, X-Access-Token');self.send_header('X-Content-Type-Options','nosniff');self.end_headers();self.wfile.write(body)
    def do_OPTIONS(self): self.send_bytes(204,b'','text/plain')
    def do_POST(self):
        path=urlparse(self.path).path.rstrip('/') or '/'
        permitted={'/access/register','/api/md5/analyze','/api/md5/learn','/api/md5/import'}
        if path not in permitted:
            return self.send_bytes(404,json_bytes({'ok':False,'error':'not found'}))
        try:
            ip=visitor_ip(self)
            if not record_visit(ip,path):
                return self.send_bytes(403,json_bytes(blocked_ip_payload(ip)))
            token=self.headers.get('X-Access-Token','')
            if path!='/access/register':
                status=access_status(token)
                if not status.get('allowed'):
                    return self.send_bytes(403,json_bytes({'ok':False,'error':'Chưa được cấp quyền','status':status.get('status')}))
                fingerprint=access_fingerprint(token)
                if not md5_rate_allowed(fingerprint):
                    return self.send_bytes(429,json_bytes({'ok':False,'error':'Thao tác quá nhanh. Thử lại sau.'}))
            size=int(self.headers.get('Content-Length','0'))
            max_size=16384 if path=='/api/md5/import' else 2048
            if size<=0 or size>max_size:
                return self.send_bytes(413,json_bytes({'ok':False,'error':'Invalid payload size'}))
            payload=json.loads(self.rfile.read(size))
            if not isinstance(payload,dict):
                raise ValueError('Payload không hợp lệ')
            if path=='/access/register':result=access_register(payload.get('token'))
            elif path=='/api/md5/analyze':
                result=md5_analyze(fingerprint,payload.get('hash'))
                gate=warmup_for('sunwin:hu')
                result.update(gate)
                if not gate['warmup_ready']:
                    # The server continues computing/logging MD5, but only
                    # metadata is public before the LIVE display threshold.
                    result={'ok':True,'kind':result['kind'],'learned':result.get('learned',0),
                            **gate,'prediction':None,'signal_status':'warming',
                            'signal_reason':'Chờ 30 phiên LIVE Sunwin từ Railway sau triển khai'}
                else:
                    result['signal_status']='predict'

            elif path=='/api/md5/learn':result=md5_learn(fingerprint,payload.get('hash'),payload.get('actual'))
            else:result=md5_import(fingerprint,payload.get('labels'))
            return self.send_bytes(200 if result.get('ok') else 400,json_bytes(result))
        except (ValueError,json.JSONDecodeError,TypeError) as e:
            return self.send_bytes(400,json_bytes({'ok':False,'error':str(e)[:160] or 'Invalid request'}))
        except sqlite3.Error:
            return self.send_bytes(503,json_bytes({'ok':False,'error':'Access database unavailable'}))
    def do_GET(self):
        u=urlparse(self.path);path=u.path.rstrip('/') or '/';qs=parse_qs(u.query)
        try:
            ip=visitor_ip(self)
            if not record_visit(ip,path):
                return self.send_bytes(403,json_bytes(blocked_ip_payload(ip)))
        except (ValueError,sqlite3.Error):
            return self.send_bytes(503,json_bytes({'ok':False,'error':'Access control unavailable'}))
        if path=='/access/status':
            status=access_status(self.headers.get('X-Access-Token',''))
            return self.send_bytes(200 if status.get('ok') else 401,json_bytes(status))
        if path=='/config' or path.startswith('/api/'):
            status=access_status(self.headers.get('X-Access-Token',''))
            if not status.get('allowed'):
                return self.send_bytes(403,json_bytes({'ok':False,'error':'Chua duoc cap quyen','status':status.get('status')}))
        if path=='/':return self.send_bytes(200,json_bytes({'ok':True,'service':'DEVELOPER THANHTUNG BACKEND','mode':'24/7 learner','engine':f'OMNI-MAX · {len(STRATEGY_NAMES)} strategy catalog + pattern memory 2-12','web':'host index externally'}))
        if path=='/config':return self.send_bytes(200,json_bytes({'ok':True,**public_config()}))
        if path=='/api/sun/current':
            c=cache_public('sun_current');return self.send_bytes(200 if c.get('ok') else 503,json_bytes(c))
        if path=='/api/sun/history':
            c=cache_public('sun_history');return self.send_bytes(200 if c.get('ok') else 503,json_bytes(c))
        if path=='/api/bcr':
            c=cache_public('bcr');return self.send_bytes(200 if c.get('ok') else 503,json_bytes(c))
        if path=='/api/warmup':
            board=(qs.get('board') or ['sunwin:hu'])[0]
            # Only expose board progress; no predictions prior to readiness.
            if not (board=='sunwin:hu' or (board.startswith('baccarat:') and len(board)<=95)):
                return self.send_bytes(400,json_bytes({'ok':False,'error':'Invalid board'}))
            return self.send_bytes(200,json_bytes({'ok':True,'board':board,**warmup_for(board)}))
        if path=='/api/learning/summary':return self.send_bytes(200,json_bytes(summary_payload()))
        if path=='/api/learning/boards':return self.send_bytes(200,json_bytes({'ok':True,'boards':board_rows_payload()}))
        if path=='/api/learning/top':
            board=(qs.get('board') or [None])[0];return self.send_bytes(200,json_bytes({'ok':True,'board':board,'rows':top_algo_payload(board,30)}))
        if path=='/api/prediction/sunwin':return self.send_bytes(200,json_bytes(prediction_payload('sunwin:hu')))
        if path=='/api/prediction/bcr':
            table=(qs.get('table') or [''])[0];return self.send_bytes(200,json_bytes(prediction_payload('baccarat:'+table)))
        if path=='/api/predictions/bcr':
            with LOCK: keys=[k for k in LATEST if k.startswith('baccarat:')]
            return self.send_bytes(200,json_bytes({'ok':True,'predictions':{k:prediction_payload(k) for k in keys}}))
        if path=='/health':
            with LOCK:ls=dict(LEARN_STATUS)
            return self.send_bytes(200,json_bytes({'ok':True,'brand':CONFIG.get('brand'),'cache':{k:{'ok':v.get('ok'),'ts':v.get('ts'),'error':v.get('error')} for k,v in CACHE.items()},'learning':ls,'warmup_sunwin':warmup_for('sunwin:hu'),'summary':summary_payload()}))
        return self.send_bytes(404,json_bytes({'ok':False,'error':'not found'}))


def run_http():
    httpd=ThreadingHTTPServer(('0.0.0.0',PORT),Handler);print(f'🌐 Backend :{PORT} · API only · external web supported');httpd.serve_forever()


async def fetch_json(client,url):
    r=await client.get(url,headers={'Accept':'application/json','Cache-Control':'no-cache','User-Agent':'Mozilla/5.0'},timeout=httpx.Timeout(8.0,connect=3.0));r.raise_for_status();return r.json()


async def cache_loop():
    limits=httpx.Limits(max_connections=30,max_keepalive_connections=18,keepalive_expiry=30)
    async with httpx.AsyncClient(limits=limits,follow_redirects=True) as client:
        next_hist=next_bcr=0.0
        while True:
            cfg=public_config();now=time.monotonic();sun_data=hist_data=bcr_data=None
            try:
                try:sun_data=await fetch_json(client,cfg['sunwin_current_api'])
                except Exception:sun_data=await fetch_json(client,cfg['sunwin_current_fallback'])
                with LOCK:CACHE['sun_current']={'ok':True,'data':sun_data,'ts':time.time(),'error':''}
            except Exception as e:
                with LOCK:CACHE['sun_current']={'ok':False,'data':CACHE['sun_current'].get('data'),'ts':CACHE['sun_current'].get('ts',0),'error':str(e)[:160]}
            if now>=next_hist:
                next_hist=now+5.0
                try:
                    hist_data=await fetch_json(client,cfg['sunwin_history_api']);
                    with LOCK:CACHE['sun_history']={'ok':True,'data':hist_data,'ts':time.time(),'error':''}
                except Exception as e:
                    with LOCK:CACHE['sun_history']={'ok':False,'data':CACHE['sun_history'].get('data'),'ts':CACHE['sun_history'].get('ts',0),'error':str(e)[:160]}
            if now>=next_bcr:
                next_bcr=now+2.0
                try:
                    bcr_data=await fetch_json(client,cfg['bcr_api'])
                    with LOCK:CACHE['bcr']={'ok':True,'data':bcr_data,'ts':time.time(),'error':''}
                except Exception as e:
                    with LOCK:CACHE['bcr']={'ok':False,'data':CACHE['bcr'].get('data'),'ts':CACHE['bcr'].get('ts',0),'error':str(e)[:160]}
            if sun_data is not None or hist_data is not None or bcr_data is not None:
                await asyncio.to_thread(learn_payloads,hist_data,sun_data,bcr_data)
            await asyncio.sleep(.55)



EXPORT_TIMEZONE = timezone(timedelta(hours=7))


def _txt_time(timestamp):
    try:
        return datetime.fromtimestamp(float(timestamp), EXPORT_TIMEZONE).strftime('%H:%M:%S · %d/%m/%Y')
    except (ValueError, TypeError, OverflowError, OSError):
        return '—'


def _txt_side(board, value):
    if board.startswith('baccarat:'):
        return 'BANKER' if value == 'TÀI' else 'PLAYER' if value == 'XỈU' else str(value or '—')
    return str(value or '—')


def _txt_dice(meta):
    dice = meta.get('dice') if isinstance(meta, dict) else None
    if not isinstance(dice, (list, tuple)):
        dice = []
    parts = []
    for x in dice[:3]:
        try:
            d = int(x)
            if d < 1 or d > 6: return None
            parts.append(d)
        except (TypeError, ValueError):
            return None
    return parts if len(parts) == 3 else None


def export_history_txt(kind='all', limit=None, include_unpredicted=False):
    """Export settled predictions, or (opt-in) all observed game results.

    Historical rounds without a prediction remain explicitly *unpredicted*;
    they are never counted as correct/incorrect. No retrospective predictions
    are invented to fill a reset/replaced SQLite database.
    """
    if kind not in ('sun', 'bcr', 'all'):
        raise ValueError('Loại lịch sử không hợp lệ')
    if limit is not None:
        limit = int(limit)
        if not 1 <= limit <= 100000:
            raise ValueError('Số phiên phải từ 1 đến 100000')
    board_filter = "r.board LIKE 'sunwin:%'" if kind == 'sun' else (
        "r.board LIKE 'baccarat:%'" if kind == 'bcr' else '1=1')
    params=[]
    # Mode original: only confirmed predictions; opt-in mode: every recorded
    # settled result with NULL prediction if none was made/logged.
    if include_unpredicted:
        sql=f"""SELECT r.board,r.external_key,p.prediction,r.result AS actual,
                       p.ok,COALESCE(p.created_at,r.seen_at) AS created_at,
                       r.meta_json,r.seen_at,p.source
                FROM rounds r LEFT JOIN prediction_log p
                ON p.board=r.board AND p.external_key=r.external_key
                WHERE {board_filter} AND r.result IN ('TÀI','XỈU')
                ORDER BY {('CAST(r.external_key AS INTEGER) DESC,r.id DESC' if kind=='sun' else 'r.id DESC')}"""
    else:
        pred_filter = "p.board LIKE 'sunwin:%'" if kind=='sun' else (
            "p.board LIKE 'baccarat:%'" if kind=='bcr' else '1=1')
        sql=f"""SELECT p.board,p.external_key,p.prediction,p.actual,p.ok,
                       p.created_at,r.meta_json,r.seen_at,p.source
                FROM prediction_log p LEFT JOIN rounds r
                ON r.board=p.board AND r.external_key=p.external_key
                WHERE {pred_filter} AND p.actual IN ('TÀI','XỈU')
                ORDER BY {('CAST(p.external_key AS INTEGER) DESC' if kind=='sun' else 'p.created_at DESC,p.board DESC,p.external_key DESC')}"""
    if limit is not None:
        sql+=' LIMIT ?';params.append(limit)
    with db() as con:
        entries=[dict(x) for x in con.execute(sql,params)]
        seen_total=con.execute('SELECT COUNT(*) FROM rounds WHERE '+board_filter.replace('r.board','board'),()).fetchone()[0]
    entries.reverse()
    title={'sun':'SUNWIN','bcr':'BACCARAT','all':'SUNWIN + BACCARAT'}[kind]
    wins=sum(x['prediction'] is not None and x['prediction']==x['actual'] for x in entries)
    evaluated=sum(x['prediction'] is not None for x in entries)
    missing=len(entries)-evaluated
    live=sum(e.get('source')=='live' for e in entries)
    replay=sum(e.get('source')=='replay' for e in entries)
    lines=[
        'DEVELOPER THANHTUNG · LỊCH SỬ DỰ ĐOÁN',
        '='*44,
        f'Loại: {title}',
        f'Xuất lúc: {_txt_time(time.time())} (UTC+7)',
        f'Chế độ: {"TẤT CẢ KẾT QUẢ ĐÃ LƯU" if include_unpredicted else "CHỈ PHIÊN CÓ DỰ ĐOÁN"}',
        f'Tổng phiên đã lưu trong SQLite ({title}): {seen_total}',
        f'Số phiên trong TXT: {len(entries)}',
        f'Có dự đoán: {evaluated} | Chưa dự đoán: {missing}',
        f'ĐÚNG: {wins} | SAI: {evaluated-wins}',
        f'Chế độ dự đoán: LIVE {live} | REPLAY {replay}',
        'Phiên chưa dự đoán KHÔNG tính ĐÚNG/SAI.',
        'REPLAY là dự đoán tái dựng, không phải thắng thua LIVE.',
        'Thời gian là giờ backend ghi nhận, có thể khác giờ gốc.',
        'Lịch sử cũ mất khi đổi/xóa SQLite không thể tự khôi phục.',
        '='*44,'',
    ]
    if not entries:
        lines.extend(['Chưa có dữ liệu phù hợp trong database.',
                      'Dùng /dbstatus để kiểm tra API, SQLite và phiên cuối.'])
    last_board=None
    for r in entries:
        board=r['board']
        if board!=last_board:
            lines.extend(['',f'BACCARAT · BÀN {board.split(":",1)[1]}' if board.startswith('baccarat:') else 'SUNWIN · TÀI/XỈU','-'*34]);last_board=board
        try:
            meta=json.loads(r['meta_json'] or '{}')
            if not isinstance(meta,dict):meta={}
        except (json.JSONDecodeError,TypeError):meta={}
        identifier=meta.get('sid') if board.startswith('sunwin:') else None
        if not identifier:
            if board.startswith('baccarat:'):
                identifier=f'{meta.get("shoe") or "—"} / ván {meta.get("pos") or "—"}'
            else:identifier=r['external_key']
        lines.append(f'#{identifier}')
        lines.append(f'Thời gian ghi nhận: {_txt_time(r["seen_at"] if r["seen_at"] is not None else r["created_at"])}')
        if r['prediction'] is None:
            lines.append(f'CHƯA DỰ ĐOÁN → {_txt_side(board,r["actual"])} · CHỈ GHI KẾT QUẢ')
        else:
            correct=(r['prediction']==r['actual'])
            mode = 'LIVE' if r.get('source') == 'live' else 'REPLAY'
            lines.append(f'{_txt_side(board,r["prediction"])} → {_txt_side(board,r["actual"])} · {"ĐÚNG ✅" if correct else "SAI ❌"} · {mode}')
        if board.startswith('sunwin:'):
            dice=_txt_dice(meta)
            lines.append('Xúc xắc: '+'-'.join(map(str,dice)) if dice else 'Xúc xắc: — (API không cung cấp)')
            total=meta.get('total')
            try:
                total=int(total) if total is not None else sum(dice) if dice else None
                if total is not None and not 3<=total<=18:total=None
            except (TypeError,ValueError):total=None
            lines.append(f'Tổng: {total if total is not None else "—"}')
        lines.append('')
    suffix='_tat_ca_ket_qua' if include_unpredicted else ''
    filename=f'thanhtung_{kind}_lich_su{suffix}_{datetime.now(EXPORT_TIMEZONE):%Y%m%d_%H%M%S}.txt'
    return filename,('\n'.join(lines).rstrip()+'\n').encode('utf-8-sig'),len(entries)


async def send_txt_document(client, chat_id, kind, limit=None, include_unpredicted=False):
    filename, data, count = await asyncio.to_thread(export_history_txt, kind, limit, include_unpredicted)
    if not WEB_BOT_TOKEN:
        raise RuntimeError('WEB_BOT_TOKEN chưa cấu hình')
    # Multipart upload is required by Telegram sendDocument; the existing
    # JSON-only tg() helper cannot send binary files.
    resp = await client.post(
        f'https://api.telegram.org/bot{WEB_BOT_TOKEN}/sendDocument',
        data={'chat_id':str(chat_id),
              'caption': f'📄 {kind.upper()} · {count} {"kết quả đã lưu" if include_unpredicted else "phiên có dự đoán"} · DEVELOPER THANHTUNG'},
        files={'document':(filename, io.BytesIO(data), 'text/plain; charset=utf-8')},
        timeout=httpx.Timeout(45.0, connect=8.0),
    )
    resp.raise_for_status()
    result = resp.json()
    if not result.get('ok'):
        raise RuntimeError('Telegram từ chối gửi TXT: '+str(result.get('description','không rõ lý do'))[:150])
    return count


def _parse_txt_limit(argument):
    if not argument:
        return None
    if not argument.isdecimal():
        raise ValueError('Dùng /exportsun [số phiên], ví dụ /exportsun 500')
    value = int(argument)
    if not 1 <= value <= 100000:
        raise ValueError('Số phiên phải từ 1 đến 100000')
    return value


async def tg(client,method,payload):
    if not WEB_BOT_TOKEN:return None
    try:
        r=await client.post(f'https://api.telegram.org/bot{WEB_BOT_TOKEN}/{method}',json=payload,timeout=12);j=r.json();return j.get('result') if j.get('ok') else None
    except Exception:return None


def quant_report():
    from Viptool import summarize
    with db() as con:
        rows=con.execute("""SELECT prediction,actual,ok,external_key FROM prediction_log
            WHERE board='sunwin:hu' AND source='live'
            ORDER BY CAST(external_key AS INTEGER) DESC, created_at DESC LIMIT 200""").fetchall()
    lines=['<b>📐 KIỂM ĐỊNH THUẬT TOÁN · LIVE</b>','━━━━━━━━━━━━━━━━━━']
    for k in (20,50,100,200):
        r=summarize(list(reversed(rows[:k])))
        if not r['n']:
            lines.append(f'{k} phiên: chưa có LIVE')
            continue
        lines.append(f"{k} phiên: {r['wins']}/{r['n']} ({100*r['rate']:.1f}%)"
                     f" · Wilson 95% [{100*r['lower']:.1f}–{100*r['upper']:.1f}]%"
                     f" · H={r['entropy']:.2f} bit"
                     + (f" · H(next|2)={r['conditional_entropy']:.2f}" if r['conditional_entropy'] is not None else ''))
    lines.append('\n<i>Wilson là khoảng bất định thống kê; entropy đo độ khó đoán trong mẫu. Không phải xác suất thắng phiên tới. Chỉ LIVE, không REPLAY.</i>')
    return '\n'.join(lines)

def v10_live_report():
    """LIVE-only audit; do not mix replay into accuracy."""
    with db() as con:
        rows=con.execute("""SELECT prediction,actual,ok,external_key FROM prediction_log
            WHERE board='sunwin:hu' AND source='live'
            ORDER BY CAST(external_key AS INTEGER) DESC, created_at DESC LIMIT 200""").fetchall()
    def segment(n):
        a=rows[:n]
        if not a:return f'{n} phiên: chưa có dữ liệu LIVE'
        wins=sum(int(r['ok']) for r in a)
        tai=sum(r['actual']=='TÀI' for r in a)
        bot_tai=sum(r['prediction']=='TÀI' for r in a)
        baseline=max(tai,len(a)-tai)
        return (f'{n} phiên: <b>{wins}/{len(a)} ({wins/len(a)*100:.1f}%)</b>'
                f' · nền {baseline/len(a)*100:.1f}% · bot TÀI {bot_tai}/{len(a)}')
    return ('<b>📊 ĐÁNH GIÁ LIVE · V10</b>\n━━━━━━━━━━━━━━━━━━\n'
            + '\n'.join(segment(n) for n in (20,50,100,200))
            + '\n\n<i>Nền là tỷ lệ phía xuất hiện nhiều hơn trong mẫu đã xảy ra, không phải chiến lược tương lai. REPLAY không được tính.</i>')

def admin_keyboard(page='home'):
    """Compact, consistent inline navigation. No duplicate menus or side effects."""
    def b(label, key):
        return {'text':label, 'callback_data':'adm:'+key}

    menus = {
        'home': [
            [b('📊 TỔNG QUAN', 'overview'), b('🎮 CÁC BÀN', 'boards')],
            [b('🤖 TIẾN ĐỘ AUTO 30 PHIÊN', 'warmup')],
            [b('🩺 HỆ THỐNG', 'system'), b('🌐 QUẢN LÝ IP', 'ips')],
            [b('🔐 DUYỆT QUYỀN WEB', 'access')],
            [b('⚙️ CẤU HÌNH', 'settings'), b('📥 XUẤT TXT', 'export')],
            [b('🧧 MÃ QR', 'loc'), b('❔ HƯỚNG DẪN', 'help')],
            [b('🔄 LÀM MỚI', 'home')],
        ],
        'access': [[b('🔄 TẢI YÊU CẦU', 'access')]],
        'warmup': [[b('🔄 CẬP NHẬT 30 PHIÊN','warmup')]],
        'overview': [
            [b('🎯 LIVE', 'live'), b('📐 KIỂM ĐỊNH', 'quant')],
            [b('📊 HỌC CẦU', 'stats'), b('🏆 THUẬT TOÁN', 'algo')],
            [b('📉 KIỂM TRA LỆCH', 'bias'), b('🎮 CÁC BÀN', 'boards')],
        ],
        'system': [
            [b('📡 TRẠNG THÁI API', 'api'), b('🗃 SQLITE', 'dbstatus')],
            [b('🧯 KIỂM TOÁN', 'auditdata'), b('📉 KIỂM TRA LỆCH', 'bias')],
        ],
        'health': [
            [b('📡 API', 'api'), b('🗃 SQLITE', 'dbstatus')],
            [b('🧯 KIỂM TOÁN', 'auditdata'), b('📉 LỆCH', 'bias')],
        ],
        'ips': [
            [b('🔄 CẬP NHẬT IP', 'ips'), b('❔ LỆNH IP', 'iphelp')],
        ],
        'settings': [
            [b('🔗 LINK / API', 'links'), b('📡 KIỂM TRA', 'api')],
            [b('❔ LỆNH CÀI ĐẶT', 'settingshelp')],
        ],
        'export': [
            [b('📄 SUNWIN', 'exportsun'), b('📄 BACCARAT', 'exportbcr')],
            [b('📚 SUNWIN · TẤT CẢ', 'exportsunall')],
            [b('📚 BACCARAT · TẤT CẢ', 'exportbcrall')],
            [b('📦 XUẤT CẢ HAI', 'exportall')],
        ],
        'live': [[b('📐 KIỂM ĐỊNH', 'quant'), b('📉 KIỂM TRA LỆCH', 'bias')]],
        'quant': [[b('🎯 LIVE', 'live'), b('📊 HỌC CẦU', 'stats')]],
        'stats': [[b('🏆 THUẬT TOÁN', 'algo'), b('🎮 CÁC BÀN', 'boards')]],
        'algo': [[b('📊 HỌC CẦU', 'stats'), b('📐 KIỂM ĐỊNH', 'quant')]],
        'boards': [[b('🎯 LIVE', 'live'), b('📊 HỌC CẦU', 'stats')]],
        'api': [[b('🗃 SQLITE', 'dbstatus'), b('⚙️ LINK / API', 'links')]],
        'dbstatus': [[b('📡 API', 'api'), b('🧯 KIỂM TOÁN', 'auditdata')]],
        'auditdata': [[b('🗃 SQLITE', 'dbstatus'), b('📉 KIỂM TRA LỆCH', 'bias')]],
        'bias': [[b('🎯 LIVE', 'live'), b('📐 KIỂM ĐỊNH', 'quant')]],
        'links': [[b('⚙️ CẤU HÌNH', 'settings'), b('❔ CÁCH SỬA', 'settingshelp')]],
        'help': [[b('🌐 LỆNH IP', 'iphelp'), b('⚙️ LỆNH LINK', 'settingshelp')]],
        'iphelp': [[b('🌐 DANH SÁCH IP', 'ips')]],
        'settingshelp': [[b('🔗 LINK ĐANG DÙNG', 'links')]],
    }
    rows = list(menus.get(page, menus['home']))
    if page == 'ips':
        # One-tap admin actions for recently seen IPs; IPv4/IPv6 fit callback limits.
        try:
            with db() as con:
                recent=con.execute('''SELECT v.ip, CASE WHEN b.ip IS NULL THEN 0 ELSE 1 END banned
                      FROM visitor_ips v LEFT JOIN banned_ips b ON v.ip=b.ip
                      ORDER BY v.last_seen DESC LIMIT 6''').fetchall()
                known={r['ip'] for r in recent}
                extra=con.execute('SELECT ip FROM banned_ips ORDER BY banned_at DESC LIMIT 6').fetchall()
                recent=list(recent)+[{'ip':r['ip'],'banned':1} for r in extra if r['ip'] not in known]
            for r in recent[:9]:
                ip=r['ip'];blocked=bool(r['banned'])
                label=('✅ MỞ ' if blocked else '⛔ CHẶN ')+ip
                action=('unbanip:' if blocked else 'banip:')+ip
                if len(('adm:'+action).encode('utf-8'))<=64:
                    rows.append([b(label,action)])
        except sqlite3.Error:
            pass
    if page != 'home':
        if page not in ('ips', 'export', 'settings', 'overview', 'system'):
            rows.append([b('🔄 LÀM MỚI', page)])
        rows.append([b('🏠 TRANG CHỦ', 'home')])
    return {'inline_keyboard': rows}


def admin_home():
    s = summary_payload()
    now = time.time()
    with LOCK:
        apis = {k:dict(v) for k,v in CACHE.items()}
        state = dict(LEARN_STATUS)
    fresh = sum(bool(v.get('ok') and now-v.get('ts', 0)<120) for v in apis.values())
    api_text = '🟢 3/3' if fresh==3 else ('🟡 '+str(fresh)+'/3') if fresh else '🔴 0/3'
    uptime = max(0,int(now-state.get('started',now)))
    live = '—' if s['live_win_rate'] is None else f"{s['live_win_rate']}%"
    stamp = datetime.fromtimestamp(now, timezone(timedelta(hours=7))).strftime('%H:%M · %d/%m/%Y')
    return (
        '<b>✦ DEVELOPER THANHTUNG ✦</b>\n'
        '<b>TRUNG TÂM QUẢN TRỊ</b>\n'
        '━━━━━━━━━━━━━━━━━━━━\n'
        f'📡 Nguồn API mới: <b>{api_text}</b>\n'
        f'⏱ Worker: <b>{uptime//3600}h {(uptime%3600)//60}m</b>\n'
        f'🗂 Đã lưu: <b>{s["rounds"]:,}</b> phiên · <b>{s["boards"]}</b> bàn\n'
        f'🧠 Mẫu đã học: <b>{s["patterns"]:,}</b>\n'
        '━━━━━━━━━━━━━━━━━━━━\n'
        f'🎯 LIVE: <b>{s["live_predictions"]:,}</b> · <b>{live}</b>\n'
        f'📼 REPLAY: <b>{s["replay_predictions"]:,}</b>\n'
        f'🤖 AUTO 30: <b>{warmup_for("sunwin:hu")["warmup_count"]}/30</b> · /warmup\n'
        f'🕒 <i>{stamp} (GMT+7)</i>\n\n'
        '<i>Chạm menu bên dưới để quản lý. LIVE khác REPLAY; tỷ lệ cũ không đảm bảo phiên sau.</i>'
    )


def admin_overview_text():
    return ('<b>📊 TRUNG TÂM THỐNG KÊ</b>\n━━━━━━━━━━━━━━━━━━━━\n'
            '🎯 LIVE: dữ liệu dự đoán thực trước khi chốt phiên.\n'
            '📐 Kiểm định: thống kê 20–200 phiên LIVE.\n'
            '📊 Học cầu: số mẫu và trạng thái worker.\n'
            '🏆 Thuật toán: kết quả kiểm tra có hiệu chỉnh.\n'
            '📉 Lệch: phát hiện thiên lệch chọn TÀI/XỈU.\n'
            '🎮 Các bàn: tình trạng dữ liệu của mỗi bàn.\n\n'
            '<i>Chọn chức năng để xem số liệu thực từ SQLite.</i>')


def admin_system_text():
    with LOCK:
        cs = {k:dict(v) for k,v in CACHE.items()}
        st = dict(LEARN_STATUS)
    now = time.time()
    def status(key):
        x=cs[key]
        if x.get('ok') and now-x.get('ts', 0)<120:return '🟢 Mới'
        if x.get('ok'):return '🟡 Cũ'
        return '🔴 Lỗi/chưa tải'
    return ('<b>🩺 TRẠNG THÁI HỆ THỐNG</b>\n━━━━━━━━━━━━━━━━━━━━\n'
            f'📡 Sunwin · current: {status("sun_current")}\n'
            f'📜 Sunwin · history: {status("sun_history")}\n'
            f'🎴 Baccarat: {status("bcr")}\n'
            f'🔄 Chu kỳ worker: <b>{st.get("cycles",0):,}</b>\n'
            f'🛑 Phiên bỏ qua: <b>{st.get("stale_skipped",0):,}</b>\n\n'
            'Dùng các nút để kiểm tra API, SQLite và mâu thuẫn dữ liệu.\n'
            '<i>Trạng thái API không khẳng định game đang hoạt động.</i>')


def admin_settings_text():
    return ('<b>⚙️ CẤU HÌNH GAME & API</b>\n━━━━━━━━━━━━━━━━━━━━\n'
            '🔗 Xem URL Sunwin và các API đang dùng.\n'
            '🛠 Sửa link bằng lệnh có URL hợp lệ.\n'
            '🔐 Chỉ admin được phép cập nhật.\n\n'
            '<b>Ví dụ</b>\n'
            '<code>/setsunlink https://play.sunwin.webcam/?affId=Sunwin</code>\n\n'
            '<i>Đổi link có thể ảnh hưởng client Netlify nếu HTML đang dùng link riêng.</i>')


def admin_ip_help_text():
    return ('<b>🌐 QUẢN LÝ TRUY CẬP IP</b>\n━━━━━━━━━━━━━━━━━━━━\n'
            '🔍 Xem chi tiết: <code>/ip 1.2.3.4</code>\n'
            '⛔ Chặn: <code>/banip 1.2.3.4</code>\n'
            '✅ Mở chặn: <code>/unbanip 1.2.3.4</code>\n'
            '⚡ Có nút CHẶN/MỞ nhanh trong mục IP.\n\n'
            '<i>Số phút là ước tính theo request tới Railway, không phải tổng thời gian chơi thực tế trên Netlify.</i>')


def admin_settings_help_text():
    return ('<b>⚙️ LỆNH THIẾT LẬP</b>\n━━━━━━━━━━━━━━━━━━━━\n'
            '🎮 Sunwin: <code>/setsunlink URL</code>\n'
            '📡 API current: <code>/setsunapi URL</code>\n'
            '📜 API history: <code>/setsunhistory URL</code>\n'
            '🛰 API fallback: <code>/setsunfallback URL</code>\n'
            '🎴 Baccarat: <code>/setbcrapi URL</code>\n'
            '🔄 Khôi phục mặc định: <code>/resetlinks</code>\n\n'
            '<i>Chỉ sử dụng /resetlinks khi chắc chắn muốn bỏ các URL đã chỉnh.</i>')


def admin_help_text():
    return ('<b>❔ HƯỚNG DẪN BOT ADMIN</b>\n━━━━━━━━━━━━━━━━━━━━\n'
            '🏠 <code>/start</code> hoặc <code>/menu</code> · Bảng điều khiển\n'
            '📊 <code>/stats</code> · Thống kê học nền\n'
            '🎯 <code>/live</code> · Kiểm định LIVE\n'
            '🩺 <code>/diagnose</code> · Chẩn đoán\n'
            '🌐 <code>/ips</code> · Danh sách IP\n'
            '⚙️ <code>/links</code> · Link/API\n'
            '📥 <code>/exportmenu</code> · Xuất TXT\n'
            '🧧 <code>/qr</code> · Gửi ảnh QR\n\n'
            '<i>Để thao tác nhanh, hãy dùng các nút ngay dưới tin nhắn.</i>')

def model_health_text():
    """Shows factual, settled predicted-vs-actual imbalance (not a prediction)."""
    try:
        with db() as con:
            rows=con.execute("""SELECT prediction,actual,ok,source FROM prediction_log
                    WHERE board='sunwin:hu' ORDER BY created_at DESC LIMIT 100""").fetchall()
            latest=con.execute("SELECT external_key FROM rounds WHERE board='sunwin:hu' ORDER BY CAST(external_key AS INTEGER) DESC LIMIT 1").fetchone()
        def fmt(n):
            segment=rows[:n]
            if not segment:return '—'
            n0=len(segment);won=sum(int(x['ok']) for x in segment)
            pt=sum(x['prediction']=='TÀI' for x in segment)
            at=sum(x['actual']=='TÀI' for x in segment)
            return f'{won}/{n0} ({100*won/n0:.1f}%) · bot TÀI {pt}/{n0} · thực TÀI {at}/{n0}'
        def skew(n):
            z=rows[:n]
            return len(z)>=20 and abs(sum(x['prediction']=='TÀI' for x in z)/len(z)-.5)>.25
        warn='\n⚠️ <b>Thiên lệch dự đoán: đã phát hiện</b>' if skew(50) else ''
        live=[r for r in rows if r['source']=='live']
        live_line=('Chưa đủ bản ghi live' if not live else
                   f'{sum(x["ok"] for x in live)}/{len(live)} ({100*sum(x["ok"] for x in live)/len(live):.1f}%)')
        return ('<b>📉 KIỂM TRA LỆCH DỰ ĐOÁN</b>\n━━━━━━━━━━━━━━━━━━\n'
                +f'📍 Phiên cuối: <b>{html.escape(str(latest[0])) if latest else "—"}</b>\n'
                +f'20 phiên: {fmt(20)}\n'
                +f'50 phiên: {fmt(50)}\n'
                +f'100 phiên: {fmt(100)}\n'
                +f'🟢 Dự đoán LIVE: <b>{live_line}</b>\n'
                +warn+'\n\n<i>LIVE là dự đoán lưu trước khi có kết quả; các số 20/50/100 có thể chứa bản ghi REPLAY (tái dựng).</i>')
    except Exception as exc:return '❌ Lỗi đọc SQLite: '+html.escape(str(exc))


def export_menu_text():
    return ('<b>📄 XUẤT LỊCH SỬ · TXT</b>\n━━━━━━━━━━━━━━━━━━\n'
            '📥 TXT SUNWIN/BCR: chỉ phiên <b>có dự đoán</b>.\n'
            '📚 TOÀN BỘ: cả phiên không dự đoán.\n\n'
            'Dùng <code>/exportsun 500</code> hoặc <code>/exportbcr 500</code> '
            'để giới hạn số phiên.\n<i>Không tạo dữ liệu hoặc dự đoán giả.</i>')


def health_menu_text():
    return ('<b>🩺 CHẨN ĐOÁN HỆ THỐNG</b>\n━━━━━━━━━━━━━━━━━━\n'
            '🛰 API: dữ liệu online có cập nhật không?\n'
            '🗃️ SQLite: phiên đã lưu và phiên cuối?\n'
            '📉 Lệch: bot có chọn TÀI/XỈU bất thường?\n'
            '🧯 Kiểm toán: nguồn API có trùng phiên mâu thuẫn?\n\n'
            '<i>Chọn bên dưới để xem chi tiết.</i>')


def api_status_text():
    with LOCK:c={k:dict(v) for k,v in CACHE.items()}
    def one(k,label):
        v=c[k];age=int(time.time()-v.get('ts',0)) if v.get('ts') else -1
        return f'{"🟢" if v.get("ok") else "🔴"} {label} · {age}s' + (f'\n└ {html.escape(str(v.get("error")))}' if not v.get('ok') and v.get('error') else '')
    return '<b>📡 API STATUS</b>\n'+one('sun_current','SUN CURRENT')+'\n'+one('sun_history','SUN HISTORY')+'\n'+one('bcr','BCR')


def db_status_text():
    """Admin-only diagnostic for stale API / fresh database / missing predictions."""
    with db() as con:
        allr=con.execute("SELECT COUNT(*) n FROM rounds WHERE board LIKE 'sunwin:%'").fetchone()['n']
        allp=con.execute("SELECT COUNT(*) n FROM prediction_log WHERE board LIKE 'sunwin:%'").fetchone()['n']
        last=con.execute("SELECT external_key,seen_at FROM rounds WHERE board LIKE 'sunwin:%' ORDER BY CAST(external_key AS INTEGER) DESC LIMIT 1").fetchone()
        lastpred=con.execute("SELECT external_key,created_at FROM prediction_log WHERE board LIKE 'sunwin:%' ORDER BY CAST(external_key AS INTEGER) DESC LIMIT 1").fetchone()
        audit=guard.diagnostics(con,'sunwin:hu')
    with LOCK:
        caches={k:dict(v) for k,v in CACHE.items()}
        learn=dict(LEARN_STATUS)
    def api_round(kind):
        x=caches.get(kind,{})
        if not x.get('ok'):
            return 'LỖI: '+str(x.get('error') or 'không nhận được API')[:90]
        try:
            found=normalize_sun(x.get('data')) if kind=='sun_history' else normalize_sun_current(x.get('data'))
            if not found: return 'Không đọc được ID/kết quả phiên từ API'
            return '#'+str(found[-1]['key'])
        except Exception as e:
            return 'Lỗi parse: '+str(e)[:80]
    new_ts=learn.get('last_new_round_at') or 0
    age='—' if not new_ts else str(int((time.time()-new_ts)//60))+' phút trước'
    return ('<b>🧰 KIỂM TRA LỊCH SỬ / SQLITE</b>\n━━━━━━━━━━━━━━━━━━\n'
            +f'📁 DB_PATH: <code>{html.escape(DB_PATH)}</code>\n'
            +f'📚 SUNWIN đã lưu: <b>{allr}</b> kết quả\n'
            +f'🎯 SUNWIN có dự đoán: <b>{allp}</b>\n'
            +f'📍 Phiên SQLite mới nhất: <b>{html.escape(str(last["external_key"])) if last else "—"}</b>\n'
            +f'🧠 Phiên có dự đoán mới nhất: <b>{html.escape(str(lastpred["external_key"])) if lastpred else "—"}</b>\n'
            +f'🛰 SUN CURRENT: <b>{html.escape(api_round("sun_current"))}</b>\n'
            +f'🛰 SUN HISTORY: <b>{html.escape(api_round("sun_history"))}</b>\n'
            +f'⏱ Lần ghi phiên mới: {age}\n'
            +f'🧯 Mâu thuẫn dữ liệu đã phát hiện: <b>{audit["conflicts"]}</b>\n'
            +f'🛑 Quan sát lỗi: {learn.get("rejected",0)} · phiên quá cũ bỏ qua: {learn.get("stale_skipped",0)}\n'
            +f'⚠️ Lỗi học gần nhất: {html.escape(learn.get("last_error") or "Không")}\n\n'
            +'<i>API trả phiên cũ: cần sửa API. SQLite ít phiên: kiểm tra Volume/DB_PATH. '
            +'Chỉ có 72 dự đoán: backfill lúc khởi tạo từng có giới hạn 72; '
            +'dùng /exportsunall để xuất cả phiên không được dự đoán.</i>')



def data_audit_text():
    with db() as con:
        d=guard.diagnostics(con)
        c=con.execute("SELECT COUNT(*) FROM rounds WHERE board='sunwin:hu'").fetchone()[0]
        live=con.execute("SELECT COUNT(*) FROM prediction_log WHERE board='sunwin:hu' AND source='live'").fetchone()[0]
        replay=con.execute("SELECT COUNT(*) FROM prediction_log WHERE board='sunwin:hu' AND source!='live'").fetchone()[0]
    lines=['<b>🧯 KIỂM TOÁN DỮ LIỆU V8</b>','━━━━━━━━━━━━━━━━━━',
           f'📚 SUNWIN đã lưu: <b>{c}</b>',
           f'🟢 Dự đoán LIVE: <b>{live}</b>',
           f'📼 Dự đoán REPLAY: <b>{replay}</b>',
           f'⚠️ Mâu thuẫn đã phát hiện: <b>{d["conflicts"]}</b>']
    for x in d['recent'][:6]:
        reason=html.escape(str(x['reason']))
        lines.append(f'• <code>{html.escape(str(x["external_key"]))}</code> · {reason} ({html.escape(x["source"])})')
    if not d['recent']:
        lines.append('Chưa ghi nhận mâu thuẫn từ lúc cài V8. Dữ liệu cũ chưa được xác minh lại.')
    lines.append('\n<i>Dữ liệu sai được cách ly; V8 không tự sửa/xóa lịch sử SQLite cũ. API chưa cung cấp nguồn xác thực độc lập.</i>')
    return '\n'.join(lines)[:3900]


def warmup_admin_text():
    sun=warmup_for('sunwin:hu')
    with db() as con:
        boards=[r[0] for r in con.execute("SELECT DISTINCT board FROM rounds WHERE board LIKE 'baccarat:%' ORDER BY board LIMIT 18")]
    lines=['<b>🤖 AUTO DỰ ĐOÁN 24/24 · ĐỒNG BỘ SAU DEPLOY</b>',
           '━━━━━━━━━━━━━━━━━━━━',
           f'🎲 SUNWIN: <b>{sun["warmup_count"]}/{sun["warmup_target"]}</b> · '+('✅ ĐÃ HIỆN' if sun['warmup_ready'] else '⏳ ĐANG CHẠY NGẦM')]
    for board in boards:
        progress=warmup_for(board)
        lines.append(f'🃏 {html.escape(board[9:])}: <b>{progress["warmup_count"]}/{progress["warmup_target"]}</b> · '+('✅' if progress['warmup_ready'] else '⏳'))
    lines.append('\n<i>Chỉ đếm LIVE đã phát dự đoán trước khi kết quả được ghi nhận; không cộng phiên tải bù. Backend luôn tiếp tục dự đoán, kể cả khi web chưa hiện.</i>')
    return '\n'.join(lines)[:3900]


def stats_text():
    s=summary_payload();rate='—' if s['win_rate'] is None else f"{s['win_rate']}%";up=int(time.time()-s['learner']['started'])
    live_rate='—' if s['live_win_rate'] is None else f"{s['live_win_rate']}%"
    return (f'<b>📊 THỐNG KÊ HỌC NỀN 24/7</b>\n━━━━━━━━━━━━━━━━━━\n'
            f'📚 Phiên đã học: <b>{s["rounds"]}</b>\n🎮 Số bàn: <b>{s["boards"]}</b>\n〽️ Pattern unique: <b>{s["patterns"]}</b>\n'
            f'🔥 Pattern đủ ≥5 mẫu: <b>{s["mature_patterns"]}</b>\n🧩 Tổng lượt học pattern: <b>{s["pattern_samples"]}</b>\n'
            f'🧠 Strategy đang có log: <b>{s["strategies"]}/{len(STRATEGY_NAMES)}</b>\n🧩 Auto cầu: <b>{s["auto_cau_strategies"]}</b> chiến lược · {s["auto_cau_logs"]} lượt kiểm\n🎯 Dự đoán walk-forward: <b>{s["settled_predictions"]}</b>\n'
            f'✅ Tổng gồm REPLAY: <b>{s["wins"]}</b> · {rate}\n'
            f'🟢 LIVE: {s["live_wins"]}/{s["live_predictions"]} · {live_rate}\n'
            f'📼 REPLAY: <b>{s["replay_predictions"]}</b> lượt\n♻️ Chu kỳ worker: <b>{s["learner"]["cycles"]}</b>\n⏱ Uptime: <b>{up//3600}h {(up%3600)//60}m</b>\n\n'
            '<i>% là thống kê lịch sử đã chốt, không phải bảo đảm phiên sau.</i>')


def boards_text():
    rows=board_rows_payload();lines=['<b>🎮 BÀN / CẦU ĐÃ HỌC</b>','━━━━━━━━━━━━━━━━━━']
    for r in rows[:30]:
        rate='—' if r['win_rate'] is None else f"{r['win_rate']}%";name=html.escape(r['board'].replace('baccarat:','BCR · '))
        
        with LOCK: lp=dict(LATEST.get(r['board']) or {})
        if r['board'].startswith('baccarat:'):
            pv='BANKER' if lp.get('prediction')=='TÀI' else 'PLAYER' if lp.get('prediction')=='XỈU' else '—'
        else:
            pv=lp.get('prediction') or '—'
        cf=f"{lp.get('confidence')}%" if lp.get('confidence') is not None else '—'
        lines.append(f'• <b>{name}</b>\n  {r["rounds"]} phiên · {r["patterns"]} cầu · chín {r["mature"]} · {rate}\n  NEXT: <b>{pv}</b> · {cf}')
    if not rows:lines.append('Chưa có dữ liệu.')
    return '\n'.join(lines)[:3900]


def algo_text():
    rows=top_algo_payload(None,20);lines=['<b>🏆 TOP THUẬT TOÁN ĐÃ KIỂM TRA</b>','━━━━━━━━━━━━━━━━━━']
    for i,r in enumerate(rows,1):lines.append(f'{i:02d}. <code>{r["strategy"]}</code>\n    {r["wins"]}/{r["n"]} · raw {r["rate"]}% · hiệu chỉnh {r["bayes"]}%')
    if not rows:lines.append('Chưa đủ dữ liệu.')
    lines.append('\n<i>Xếp hạng dùng Bayes smoothing để tránh model ít mẫu leo top giả.</i>');return '\n'.join(lines)[:3900]


def links_text():
    cfg=public_config()
    return (f'<b>🔗 LINK / API</b>\n━━━━━━━━━━━━━━━━━━\n<b>SUN GAME</b>\n<code>{html.escape(cfg["sunwin_game_url"])}</code>\n\n'
            f'<b>SUN CURRENT</b>\n<code>{html.escape(cfg["sunwin_current_api"])}</code>\n\n<b>SUN HISTORY</b>\n<code>{html.escape(cfg["sunwin_history_api"])}</code>\n\n'
            f'<b>BCR API</b>\n<code>{html.escape(cfg["bcr_api"])}</code>\n\n<code>/setsunlink URL</code>\n<code>/setsunapi URL</code>\n<code>/setsunhistory URL</code>\n<code>/setbcrapi URL</code>')


async def send_loc_photo(client,chat_id):
    """Gui QR goc tu du lieu nhung san, khong can loc-qr.jpg tren GitHub."""
    if not WEB_BOT_TOKEN:
        return False
    try:
        import io
        from qr_asset import qr_image_bytes
        photo = io.BytesIO(qr_image_bytes())
        response = await client.post(
            f'https://api.telegram.org/bot{WEB_BOT_TOKEN}/sendPhoto',
            data={
                'chat_id': str(chat_id),
                'caption': '🧧 <b>ĂN NHỚ LỘC</b> 🧧\n━━━━━━━━━━━━━━━━━━\nCảm ơn bạn đã ủng hộ! ❤️\n<i>Hoàn toàn tự nguyện.</i>',
                'parse_mode': 'HTML',
            },
            files={'photo': ('loc-qr.jpg', photo, 'image/jpeg')},
            timeout=25,
        )
        return bool(response.json().get('ok'))
    except Exception:
        return False

async def send_admin(client,chat_id,text,keyboard=True,page='home'):
    p={'chat_id':chat_id,'text':text,'parse_mode':'HTML','disable_web_page_preview':True}
    if keyboard:p['reply_markup']=admin_keyboard(page)
    return await tg(client,'sendMessage',p)


async def handle_admin_message(client,m):
    chat_id=(m.get('chat') or {}).get('id');uid=(m.get('from') or {}).get('id');text=(m.get('text') or '').strip()
    if not chat_id or not text:return
    if uid not in ADMIN_IDS:
        if text.startswith('/start'):await send_admin(client,chat_id,'⛔ Bot quản trị riêng.',False)
        return
    parts=text.split(maxsplit=1);cmd=parts[0].split('@',1)[0].lower();arg=parts[1].strip() if len(parts)>1 else ''
    if cmd in ('/loc','/qr'):
        if not await send_loc_photo(client,chat_id):await send_admin(client,chat_id,'Không gửi được ảnh QR.',False)
        return
    try:
        if cmd in ('/start','/menu'):out=admin_home()
        elif cmd in ('/panel','/overview'):out=admin_overview_text()
        elif cmd in ('/help','/huongdan'):out=admin_help_text()
        elif cmd=='/settings':out=admin_settings_text()
        elif cmd=='/system':out=admin_system_text()
        elif cmd in ('/status','/api'):out=api_status_text()
        elif cmd in ('/stats','/learn'):out=stats_text()
        elif cmd in ('/warmup','/auto30'):out=warmup_admin_text()
        elif cmd in ('/live','/liveaudit'):out=v10_live_report()
        elif cmd in ('/loc','/qr'):out='🧧 Ăn Nhớ Lộc 🧧\nỦng hộ tự nguyện.'
        elif cmd in ('/quant','/modelcheck'):out=quant_report()
        elif cmd in ('/yeucau','/quyenweb'):out=access_pending_text()
        elif cmd in ('/capquyen','/thuhoi'):
            out=access_set(arg,cmd=='/capquyen')
        elif cmd=='/ips':out=ip_overview()
        elif cmd=='/ip':
            if not arg:raise ValueError('Dùng /ip 1.2.3.4')
            out=ip_detail(arg)
        elif cmd in ('/banip','/unbanip'):
            if not arg:raise ValueError('Dùng /banip 1.2.3.4')
            target=arg.split()[0]
            ip,ban,info=ip_action(target,'ban' if cmd=='/banip' else 'unban',
                                  ' '.join(arg.split()[1:]))
            out=('⛔ Đã chặn ' if cmd=='/banip' else '✅ Đã mở ')+f'<code>{html.escape(ip)}</code>'
        elif cmd=='/boards':out=boards_text()
        elif cmd in ('/topalgo','/algo'):out=algo_text()
        elif cmd=='/links':out=links_text()
        elif cmd=='/dbstatus':out=db_status_text()
        elif cmd in ('/auditdata','/dataaudit'):out=data_audit_text()
        elif cmd in ('/bias','/modelhealth'):out=model_health_text()
        elif cmd=='/diagnose':out=admin_system_text()
        elif cmd=='/exportmenu':out=export_menu_text()
        elif cmd in ('/exportsun','/txtsun','/exportbcr','/txtbcr','/exportall','/txtall',
                     '/exportsunall','/exportbcrall','/exportallresults'):
            kind = 'sun' if cmd in ('/exportsun','/txtsun','/exportsunall') else 'bcr' if cmd in ('/exportbcr','/txtbcr','/exportbcrall') else 'all'
            include_all=cmd in ('/exportsunall','/exportbcrall','/exportallresults')
            limit = _parse_txt_limit(arg)
            count = await send_txt_document(client, chat_id, kind, limit, include_all)
            out = f'✅ Đã xuất TXT {kind.upper()} · {count} {"kết quả đã lưu" if include_all else "phiên có dự đoán"}.'
        elif cmd=='/resetlinks':
            with LOCK:CONFIG.clear();CONFIG.update(DEFAULT_CONFIG)
            save_config();out='✅ Đã khôi phục link/API mặc định.'
        else:
            key={'/setsunlink':'sunwin_game_url','/setsunapi':'sunwin_current_api','/setsunhistory':'sunwin_history_api','/setsunfallback':'sunwin_current_fallback','/setbcrapi':'bcr_api'}.get(cmd)
            if key:
                if not arg:raise ValueError('Thiếu URL.')
                arg=safe_url(arg)
                with LOCK:CONFIG[key]=arg
                save_config();out=f'✅ Đã cập nhật <b>{key}</b>\n<code>{html.escape(arg)}</code>'
            else:out=admin_home()
    except Exception as e:out='❌ '+html.escape(str(e))
    page=({'/bias':'bias','/modelhealth':'bias','/diagnose':'system','/exportmenu':'export',
           '/loc':'loc','/qr':'loc','/quant':'quant','/modelcheck':'quant','/ips':'ips','/ip':'ips','/banip':'ips','/unbanip':'ips','/live':'live','/liveaudit':'live','/dbstatus':'dbstatus','/auditdata':'auditdata','/dataaudit':'auditdata','/stats':'stats','/learn':'stats','/boards':'boards',
           '/panel':'overview','/overview':'overview','/help':'help','/huongdan':'help','/settings':'settings','/system':'system',
           '/warmup':'warmup','/auto30':'warmup','/yeucau':'access','/quyenweb':'access','/capquyen':'access','/thuhoi':'access', '/algo':'algo','/topalgo':'algo','/api':'api','/status':'api','/links':'links'}).get(cmd,'home')
    await send_admin(client,chat_id,out,True,page)


async def handle_callback(client,q):
    """Authorized, single-message navigation; report recoverable error inline."""
    uid=(q.get('from') or {}).get('id')
    msg=q.get('message') or {}
    chat_id=(msg.get('chat') or {}).get('id')
    data=q.get('data') or ''
    if uid not in ADMIN_IDS or not chat_id or not data.startswith('adm:'):
        await tg(client,'answerCallbackQuery',{'callback_query_id':q.get('id'),
                                               'text':'⛔ Bạn không có quyền quản trị.', 'show_alert':True})
        return
    page=data[4:]
    if page.startswith('banip:') or page.startswith('unbanip:'):
        action,ip=page.split(':',1)
        try:
            ip_action(ip,'ban' if action=='banip' else 'unban','Admin chặn qua Telegram')
            await tg(client,'answerCallbackQuery',{'callback_query_id':q.get('id'),
                     'text':'⛔ Đã chặn IP' if action=='banip' else '✅ Đã mở IP'})
            payload={'chat_id':chat_id,'message_id':msg.get('message_id'),
                     'text':ip_overview()[:3900],'parse_mode':'HTML',
                     'disable_web_page_preview':True,'reply_markup':admin_keyboard('ips')}
            if not msg.get('message_id') or await tg(client,'editMessageText',payload) is None:
                await send_admin(client,chat_id,payload['text'],True,'ips')
        except Exception as exc:
            await tg(client,'answerCallbackQuery',{'callback_query_id':q.get('id'),
                     'text':str(exc)[:170], 'show_alert':True})
        return
    if page.startswith('grant:') or page.startswith('revoke:'):
        command,code=page.split(':',1)
        try:
            result=access_set(code,command=='grant')
            await tg(client,'answerCallbackQuery',{'callback_query_id':q.get('id'),'text':'✅ Đã xử lý yêu cầu'})
            await send_admin(client,chat_id,result,True,'access')
        except Exception as exc:
            await tg(client,'answerCallbackQuery',{'callback_query_id':q.get('id'),'text':str(exc)[:170], 'show_alert':True})
        return
    await tg(client,'answerCallbackQuery',{'callback_query_id':q.get('id')})
    if page=='loc':
        if not await send_loc_photo(client,chat_id):
            await send_admin(client,chat_id,'❌ Không gửi được QR. Kiểm tra qr_asset.py và token Telegram.',True,'home')
        return
    if page in ('exportsun','exportbcr','exportsunall','exportbcrall','exportall'):
        kind='sun' if page in ('exportsun','exportsunall') else 'bcr' if page in ('exportbcr','exportbcrall') else 'all'
        include_all=page in ('exportsunall','exportbcrall')
        try:
            count=await send_txt_document(client,chat_id,kind,include_unpredicted=include_all)
            result=f'✅ Đã gửi TXT <b>{kind.upper()}</b> · <b>{count}</b> phiên.'
        except Exception as exc:
            result='❌ Không xuất được TXT: '+html.escape(str(exc))
        await send_admin(client,chat_id,result,True,'export')
        return
    pages={
        'home':admin_home, 'overview':admin_overview_text, 'system':admin_system_text, 'access':access_pending_text,
        'warmup':warmup_admin_text,
        'settings':admin_settings_text, 'help':admin_help_text,
        'iphelp':admin_ip_help_text, 'settingshelp':admin_settings_help_text,
        'quant':quant_report, 'ips':ip_overview, 'live':v10_live_report,
        'stats':stats_text, 'boards':boards_text, 'algo':algo_text,
        'api':api_status_text, 'links':links_text, 'dbstatus':db_status_text,
        'auditdata':data_audit_text, 'bias':model_health_text,
        'health':health_menu_text, 'export':export_menu_text,
    }
    if page not in pages:page='home'
    try:
        content=pages[page]()
    except Exception as exc:
        content='❌ Không tải được mục này: '+html.escape(str(exc)[:260])
    payload={'chat_id':chat_id,'message_id':msg.get('message_id'),
             'text':content[:3900],'parse_mode':'HTML','disable_web_page_preview':True,
             'reply_markup':admin_keyboard(page)}
    if msg.get('message_id') and await tg(client,'editMessageText',payload) is not None:
        return
    # Telegram can reject 'message is not modified' on refresh; don't send a duplicate.
    if msg.get('text')==content and msg.get('reply_markup')==admin_keyboard(page):
        return
    await send_admin(client,chat_id,content[:3900],True,page)


async def bot_loop():
    if not WEB_BOT_TOKEN:
        print('ℹ️ WEB_BOT_TOKEN chưa có: learner/API vẫn chạy, bot quản trị tắt.')
        while True:await asyncio.sleep(3600)
    async with httpx.AsyncClient(limits=httpx.Limits(max_connections=30,max_keepalive_connections=20)) as client:
        await tg(client,'deleteWebhook',{'drop_pending_updates':False});await tg(client,'setMyName',{'name':'DEVELOPER THANHTUNG ADMIN'})
        await tg(client,'setMyCommands',{'commands':[
            {'command':'start','description':'🏠 Bảng điều khiển'},
            {'command':'yeucau','description':'🔐 Danh sách yêu cầu web'},
            {'command':'capquyen','description':'✅ Cấp quyền mã truy cập'},
            {'command':'thuhoi','description':'⛔ Thu hồi quyền mã truy cập'},
            {'command':'menu','description':'Mở lại menu quản trị'},
            {'command':'help','description':'Hướng dẫn sử dụng'},{'command':'loc','description':'Ảnh QR Ăn Nhớ Lộc'},{'command':'ips','description':'Xem IP truy cập'},{'command':'banip','description':'Chặn IP'},{'command':'unbanip','description':'Mở IP'},{'command':'live','description':'Đánh giá LIVE 20/50/100/200 phiên'},{'command':'stats','description':'Thống kê học cầu 24/7'},
            {'command':'boards','description':'Phiên/cầu đã học từng bàn'},{'command':'topalgo','description':'Top thuật toán đã kiểm tra'},
            {'command':'status','description':'Trạng thái API'},{'command':'links','description':'Xem link/API'},
            {'command':'exportsun','description':'Xuất TXT SUNWIN'},{'command':'exportbcr','description':'Xuất TXT Baccarat'},
            {'command':'exportall','description':'Xuất TXT tất cả'},
            {'command':'exportsunall','description':'Xuất toàn bộ kết quả SUNWIN'},
            {'command':'exportbcrall','description':'Xuất toàn bộ kết quả Baccarat'},
            {'command':'dbstatus','description':'Kiểm tra SQLite và phiên API'},
            {'command':'auditdata','description':'Kiểm toán mâu thuẫn dữ liệu'},
            {'command':'bias','description':'Chẩn đoán lệch dự đoán'},
            {'command':'diagnose','description':'Trung tâm kiểm tra'},
            {'command':'setsunlink','description':'Đổi link game SUNWIN'},{'command':'setsunapi','description':'Đổi API current SUNWIN'},
            {'command':'setsunhistory','description':'Đổi API history SUNWIN'},{'command':'setsunfallback','description':'Đổi API fallback SUNWIN'},{'command':'setbcrapi','description':'Đổi API Baccarat'}]})
        offset=0
        while True:
            try:
                updates=await tg(client,'getUpdates',{'offset':offset,'timeout':BOT_POLL_TIMEOUT,'limit':100,'allowed_updates':['message','callback_query']}) or []
                for u in updates:
                    offset=max(offset,int(u.get('update_id',0))+1)
                    if u.get('message'):await handle_admin_message(client,u['message'])
                    elif u.get('callback_query'):await handle_callback(client,u['callback_query'])
                await notify_access_requests(client)
            except asyncio.CancelledError:raise
            except Exception:await asyncio.sleep(.8)


async def main_async():
    load_config();ensure_db();init_deploy_warmup();upgrade_existing_history();load_latest_from_db();threading.Thread(target=run_http,daemon=True).start();await asyncio.gather(cache_loop(),bot_loop())


if __name__=='__main__':
    try:asyncio.run(main_async())
    except KeyboardInterrupt:pass
