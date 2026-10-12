# DEVELOPER THANHTUNG · 24/7 LEARNING BACKEND + ADMIN BOT
# External HTML can be hosted anywhere. This process provides API/cache/learning.
# Run: python maychubot.py

import os, re, json, time, math, html, asyncio, sqlite3, hashlib, threading, ipaddress, secrets
from datetime import datetime, timezone, timedelta
from pathlib import Path
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

import httpx
import sunwin_v15 as v15
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
    'updated_at':0,
}

LOCK=threading.RLock()
CONFIG={}
CACHE={
    'sun_current':{'ok':False,'data':None,'ts':0,'error':'chưa tải'},
    'sun_history':{'ok':False,'data':None,'ts':0,'error':'chưa tải'},
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
        # V18 non-destructive schema addition: one coarse browser profile per recorded IP.
        visitor_columns={row[1] for row in con.execute('PRAGMA table_info(visitor_ips)')}
        if 'user_agent' not in visitor_columns:
            con.execute("ALTER TABLE visitor_ips ADD COLUMN user_agent TEXT NOT NULL DEFAULT ''")
        if 'cpu_threads' not in visitor_columns:
            con.execute('ALTER TABLE visitor_ips ADD COLUMN cpu_threads INTEGER')
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
        CREATE TABLE IF NOT EXISTS ip_grants(
          ip TEXT PRIMARY KEY, granted_at REAL NOT NULL, expires_at REAL NOT NULL,
          granted_by INTEGER, note TEXT NOT NULL DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS idx_ip_grants_expires ON ip_grants(expires_at);
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
        v15.ensure_schema(con)
        con.commit()


# IP address from reverse proxy is trusted only if explicitly enabled.
TRUST_PROXY_IP = os.getenv('TRUST_PROXY_IP','0') == '1'
# Geo lookup is opt-in because it transmits public IP addresses to ipwho.is.
IP_GEO_ENABLED=os.getenv('IP_GEO_ENABLED','0')=='1'
GEO_CACHE={}
GEO_CACHE_LOCK=threading.Lock()

TRACKED_PATHS={'/','/access/status','/access/register','/config','/api/sun/current','/api/sun/history',
               '/api/prediction/sunwin','/api/learning/summary','/api/learning/boards',
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

# V19: IP-based temporary access. IPs may be shared by multiple people.
# This route is intentionally disabled until the reverse proxy client IP is configured.
IP_GRANT_MAX_SECONDS=365*86400

def parse_ip_duration(value):
    """Accept 30m, 2h, 2d, 7d, 48h, or 2gio/2ngay; max 365 days."""
    raw=str(value or '').strip().lower().replace(' ', '')
    aliases={'2gio':'2h','2giờ':'2h','2day':'2d','2ngay':'2d','2ngày':'2d',
             '1gio':'1h','1giờ':'1h','1ngay':'1d','1ngày':'1d'}
    raw=aliases.get(raw,raw)
    match=re.fullmatch(r'(\d{1,5})(m|min|h|hour|d|day|w|week)',raw)
    if not match:
        raise ValueError('Thời hạn: 30m, 2h, 2d, 7d, 48h hoặc 1w (tối đa 365 ngày).')
    amount=int(match.group(1));factor={'m':60,'min':60,'h':3600,'hour':3600,
             'd':86400,'day':86400,'w':604800,'week':604800}[match.group(2)]
    seconds=amount*factor
    if not 60<=seconds<=IP_GRANT_MAX_SECONDS:
        raise ValueError('Chỉ cấp từ 1 phút đến 365 ngày.')
    return seconds

def human_duration(seconds):
    seconds=max(0,int(seconds))
    if seconds>=86400:return f'{seconds//86400} ngày {seconds%86400//3600} giờ'
    if seconds>=3600:return f'{seconds//3600} giờ {seconds%3600//60} phút'
    return f'{seconds//60} phút'

def ip_grant_status(ip,now=None):
    if not TRUST_PROXY_IP or not ip:return None
    try:
        ip=str(ipaddress.ip_address(ip))
        if not ipaddress.ip_address(ip).is_global:return None
    except ValueError:return None
    now=time.time() if now is None else now
    with db() as con:
        row=con.execute('SELECT ip,granted_at,expires_at,granted_by FROM ip_grants WHERE ip=? AND expires_at>?',(ip,now)).fetchone()
    return dict(row) if row else None

def ip_grant_action(ip,action='status',duration=None,admin_id=None):
    ip=str(ipaddress.ip_address(ip))
    if action=='grant':
        if not TRUST_PROXY_IP:
            raise ValueError('Chưa bật TRUST_PROXY_IP=1 trên Railway. Không thể cấp quyền nhầm cho IP proxy.')
        if not ipaddress.ip_address(ip).is_global:
            raise ValueError('Chỉ cấp quyền IP công khai hợp lệ; không dùng IP nội bộ / proxy.')
        seconds=parse_ip_duration(duration)
        now=time.time()
        with db() as con:
            ban=con.execute('SELECT 1 FROM banned_ips WHERE ip=?',(ip,)).fetchone()
            if ban:raise ValueError('IP đang bị chặn. Phải mở chặn trước khi cấp quyền.')
            con.execute('INSERT INTO ip_grants(ip,granted_at,expires_at,granted_by,note) VALUES(?,?,?,?,?) '
                        'ON CONFLICT(ip) DO UPDATE SET granted_at=excluded.granted_at, '
                        'expires_at=excluded.expires_at,granted_by=excluded.granted_by',
                        (ip,now,now+seconds,admin_id,'Telegram admin'))
    elif action=='revoke':
        with db() as con:con.execute('DELETE FROM ip_grants WHERE ip=?',(ip,))
    return ip,ip_grant_status(ip)

def fmt_ip_grant(grant):
    if not grant:return '🔒 Chưa cấp / đã hết hạn'
    end=datetime.fromtimestamp(grant['expires_at'],timezone(timedelta(hours=7))).strftime('%H:%M · %d/%m/%Y')
    return f'✅ Còn {human_duration(grant["expires_at"]-time.time())} · hết {end} (GMT+7)'

def blocked_ip_payload(ip):
    """Return a distinct HTTP 403 state for the Netlify access screen."""
    with db() as con:
        ban=con.execute('SELECT reason FROM banned_ips WHERE ip=?',(ip,)).fetchone()
    return {'ok':False,'allowed':False,'blocked':True,'status':'ip_blocked',
            'error':'IP đã bị quản trị viên chặn','ip':ip,
            'reason':str(ban['reason'] or '')[:140] if ban else ''}

def parse_browser_profile(ua):
    """UA gives estimates only; a browser cannot reveal a reliable CPU model."""
    raw=str(ua or '')[:300]
    lowered=raw.lower()
    if 'iphone' in lowered: os_name='iOS (iPhone)'
    elif 'ipad' in lowered: os_name='iPadOS'
    elif 'android' in lowered: os_name='Android'
    elif 'windows nt' in lowered: os_name='Windows'
    elif 'mac os x' in lowered or 'macintosh' in lowered: os_name='macOS / iPadOS'
    elif 'cros' in lowered: os_name='ChromeOS'
    elif 'linux' in lowered: os_name='Linux'
    else: os_name='Không rõ'
    if 'edg/' in lowered or 'edga/' in lowered or 'edgios/' in lowered:browser='Microsoft Edge'
    elif 'opr/' in lowered or 'opera' in lowered:browser='Opera'
    elif 'firefox/' in lowered or 'fxios/' in lowered:browser='Firefox'
    elif 'crios/' in lowered or 'chrome/' in lowered:browser='Chrome'
    elif 'safari/' in lowered:browser='Safari'
    else:browser='Không rõ'
    if 'ipad' in lowered or 'tablet' in lowered:device='Máy tính bảng'
    elif 'mobile' in lowered or 'iphone' in lowered or 'android' in lowered:device='Điện thoại'
    elif raw:device='Máy tính / không xác định'
    else:device='Không có dữ liệu'
    return {'os':os_name,'browser':browser,'device':device}


def cpu_thread_hint(value):
    try:
        count=int(str(value or '').strip())
        return count if 1<=count<=128 else None
    except (ValueError,TypeError):return None


def geo_lookup(ip):
    """Coarse public-IP lookup, only on admin request and if opted in."""
    try: addr=ipaddress.ip_address(ip)
    except ValueError:return {'note':'IP không hợp lệ'}
    if not addr.is_global:
        return {'note':'IP nội bộ / proxy / địa chỉ thử nghiệm; không thể định vị'}
    if not IP_GEO_ENABLED:
        return {'note':'Chưa bật IP_GEO_ENABLED=1 trong Railway Variables'}
    now=time.time()
    with GEO_CACHE_LOCK:
        cached=GEO_CACHE.get(ip)
        if cached and cached[0]>now:return cached[1]
    try:
        response=httpx.get('https://ipwho.is/'+str(addr),timeout=httpx.Timeout(4,connect=2),follow_redirects=False)
        response.raise_for_status()
        payload=response.json()
        if not isinstance(payload,dict) or payload.get('success') is False:
            raise ValueError('GeoIP không trả kết quả')
        con=payload.get('connection') or {}
        tz=payload.get('timezone') or {}
        result={
            'country':str(payload.get('country') or '')[:80],
            'region':str(payload.get('region') or '')[:80],
            'city':str(payload.get('city') or '')[:80],
            'isp':str(con.get('isp') or '')[:100],
            'org':str(con.get('org') or '')[:100],
            'asn':str(con.get('asn') or '')[:30],
            'timezone':str(tz.get('id') or '')[:80],
        }
        ttl=12*3600
    except (httpx.HTTPError,ValueError,TypeError):
        result={'note':'Dịch vụ GeoIP tạm không phản hồi'}
        ttl=120
    with GEO_CACHE_LOCK:
        if len(GEO_CACHE)>500:GEO_CACHE.clear()
        GEO_CACHE[ip]=(now+ttl,result)
    return result


def record_visit(ip,path,user_agent='',cpu_threads=''):
    now=time.time()
    ua=str(user_agent or '')[:300].replace('\x00','')
    threads=cpu_thread_hint(cpu_threads)
    with db() as con:
        banned=con.execute('SELECT 1 FROM banned_ips WHERE ip=?',(ip,)).fetchone()
        if path in TRACKED_PATHS:
            con.execute("""INSERT INTO visitor_ips(ip,first_seen,last_seen,requests,active_seconds,last_path,user_agent,cpu_threads)
                VALUES(?,?,?,?,?,?,?,?)
                ON CONFLICT(ip) DO UPDATE SET
                  active_seconds=visitor_ips.active_seconds+
                    CASE WHEN excluded.last_seen-visitor_ips.last_seen BETWEEN 1 AND 120
                    THEN CAST(excluded.last_seen-visitor_ips.last_seen AS INTEGER) ELSE 0 END,
                  last_seen=excluded.last_seen,requests=visitor_ips.requests+1,
                  last_path=excluded.last_path,
                  user_agent=CASE WHEN excluded.user_agent<>'' THEN excluded.user_agent ELSE visitor_ips.user_agent END,
                  cpu_threads=COALESCE(excluded.cpu_threads,visitor_ips.cpu_threads)""",
                (ip,now,now,1,0,path,ua,threads))
        return not bool(banned)


# Access control V7: default deny. Browser creates a random token; the server
# only stores its SHA-256 hash, and the admin grants a human-readable request ID.
TOKEN_RE = re.compile(r'^[0-9a-f]{64}$')
CODE_RE = re.compile(r'^[A-F0-9]{10}$')

def access_fingerprint(token):
    if not isinstance(token,str) or not TOKEN_RE.fullmatch(token):
        return None
    return hashlib.sha256(token.encode('ascii')).hexdigest()

def access_status(token,ip=None):
    fingerprint=access_fingerprint(token)
    if not fingerprint:
        return {'ok':False,'status':'invalid','error':'Invalid access token'}
    now=time.time()
    # IP grant is a separate temporary entitlement; banned IP is rejected
    # by record_visit() before reaching this function on every endpoint.
    grant=ip_grant_status(ip,now)
    with db() as con:
        row=con.execute('SELECT request_code,status FROM access_requests WHERE token_hash=?',(fingerprint,)).fetchone()
        if row:con.execute('UPDATE access_requests SET last_seen=? WHERE token_hash=?',(now,fingerprint))
    if grant:
        return {'ok':True,'status':'approved','allowed':True,'access_via':'ip',
                'expires_at':grant['expires_at'],'client_ip':ip,
                'code':row['request_code'] if row else None}
    if not row:return {'ok':True,'status':'unregistered','allowed':False,'client_ip':ip}
    return {'ok':True,'status':row['status'],'code':row['request_code'],
            'allowed':row['status']=='approved','access_via':'device' if row['status']=='approved' else None,
            'client_ip':ip}

def access_register(token,ip=None):
    fingerprint=access_fingerprint(token)
    if not fingerprint:
        return {'ok':False,'status':'invalid','error':'Invalid access token'}
    grant=ip_grant_status(ip)
    if grant:
        return {'ok':True,'status':'approved','allowed':True,'access_via':'ip',
                'expires_at':grant['expires_at'],'client_ip':ip}
    now=time.time()
    with db() as con:
        r=con.execute('SELECT request_code,status FROM access_requests WHERE token_hash=?',(fingerprint,)).fetchone()
        if r:
            con.execute('UPDATE access_requests SET last_seen=? WHERE token_hash=?',(now,fingerprint))
            return {'ok':True,'status':r['status'],'code':r['request_code'],
                    'allowed':r['status']=='approved','client_ip':ip}
        for _ in range(8):
            code=secrets.token_hex(5).upper()
            try:
                con.execute('INSERT INTO access_requests(token_hash,request_code,status,created_at,last_seen) VALUES(?,?,?,?,?)',
                            (fingerprint,code,'pending',now,now))
                return {'ok':True,'status':'pending','code':code,'allowed':False,'client_ip':ip}
            except sqlite3.IntegrityError:continue
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
    msg=[f'<b>◈ HTUNG / DEVICE ACCESS</b>\n⏳ Chờ duyệt: <b>{n}</b> · ✅ Đã cấp: <b>{approved}</b>', '━━━━━━━━━━━━━━━━━━━━']
    if not pending:msg.append('Hiện chưa có yêu cầu chờ duyệt.')
    for r in pending:
        stamp=datetime.fromtimestamp(r['created_at'],timezone(timedelta(hours=7))).strftime('%d/%m %H:%M')
        msg.append(f'• <code>{r["request_code"]}</code> · {stamp}')
    msg.extend(['', '<i>Thiết bị tự tạo yêu cầu khi mở web. Bot không có mục tạo mã thủ công.</i>', '✅ <code>/capquyen MA</code> · ⛔ <code>/thuhoi MA</code>'])
    return '\n'.join(msg)

async def notify_access_requests(client):
    if not ADMIN_IDS:return
    with db() as con:
        waiting=[r['request_code'] for r in con.execute("SELECT request_code FROM access_requests WHERE status='pending' AND notified=0 ORDER BY created_at ASC LIMIT 12")]
    for code in waiting:
        payload={
          'text':f'<b>◈ THIẾT BỊ CHỜ DUYỆT</b>\n<code>{code}</code>\n⛔ Đang khóa truy cập\nDuyệt hoặc từ chối bên dưới.',
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


IP_PAGE_SIZE=6

def ip_dashboard(mode='recent',page=0):
    """Paginated, Telegram-safe IP panel. Counts are visits, not online players."""
    mode=mode if mode in ('blocked','granted') else 'recent'
    page=max(0,min(int(page),9999))
    with db() as con:
        visitors=con.execute('SELECT COUNT(*) FROM visitor_ips').fetchone()[0]
        banned=con.execute('SELECT COUNT(*) FROM banned_ips').fetchone()[0]
        active=con.execute('SELECT COUNT(*) FROM ip_grants WHERE expires_at>?',(time.time(),)).fetchone()[0]
        count=int(banned if mode=='blocked' else active if mode=='granted' else visitors)
        max_page=max(0,(count-1)//IP_PAGE_SIZE)
        page=min(page,max_page)
        if mode=='granted':
            data=con.execute('SELECT ip,expires_at FROM ip_grants WHERE expires_at>? ORDER BY expires_at ASC LIMIT ? OFFSET ?',
                (time.time(),IP_PAGE_SIZE,page*IP_PAGE_SIZE)).fetchall()
        elif mode=='blocked':
            data=con.execute("""SELECT b.ip,b.banned_at,b.reason,v.requests,v.last_seen
                FROM banned_ips b LEFT JOIN visitor_ips v ON b.ip=v.ip
                ORDER BY b.banned_at DESC LIMIT ? OFFSET ?""",
                (IP_PAGE_SIZE,page*IP_PAGE_SIZE)).fetchall()
        else:
            data=con.execute("""SELECT v.ip,v.requests,v.last_seen,
                CASE WHEN b.ip IS NULL THEN 0 ELSE 1 END banned
                FROM visitor_ips v LEFT JOIN banned_ips b ON b.ip=v.ip
                ORDER BY v.last_seen DESC LIMIT ? OFFSET ?""",
                (IP_PAGE_SIZE,page*IP_PAGE_SIZE)).fetchall()
    head={'recent':'TRUY CẬP GẦN ĐÂY','blocked':'DANH SÁCH ĐÃ CHẶN','granted':'IP ĐƯỢC CẤP QUYỀN'}[mode]
    lines=['<b>◈ HTUNG / IP CONTROL</b>',
           '<code>SECURITY  •  ACCESS MONITOR</code>',
           '━━━━━━━━━━━━━━━━━━━━',
           f'🌐 IP đã ghi nhận: <b>{visitors:,}</b>',
           f'⛔ IP đang chặn: <b>{banned:,}</b> · 🔓 Còn quyền: <b>{active:,}</b>',
           '',f'<b>▸ {head}</b>  <code>{page+1}/{max_page+1}</code>']
    if not data:
        lines.append('\n<i>Chưa có địa chỉ nào trong mục này.</i>')
    for i,r in enumerate(data,1+page*IP_PAGE_SIZE):
        ip=html.escape(r['ip'])
        if mode=='granted':
            lines.append(f'\n<b>{i:02d}.</b> 🔓 <code>{ip}</code> · còn {human_duration(r["expires_at"]-time.time())}')
        elif mode=='blocked':
            lines.append(f'\n<b>{i:02d}.</b> ⛔ <code>{ip}</code>'
                         f'\n     {int(r["requests"] or 0):,} yêu cầu đã ghi')
        else:
            flag='⛔' if r['banned'] else '🟢'
            last=datetime.fromtimestamp(r['last_seen'],timezone(timedelta(hours=7))).strftime('%H:%M %d/%m')
            lines.append(f'\n<b>{i:02d}.</b> {flag} <code>{ip}</code>'
                         f'\n     {int(r["requests"]):,} yêu cầu · {last}')
    lines.extend(['','<i>Chạm “Xem IP” để mở chi tiết và thao tác.</i>'])
    if not TRUST_PROXY_IP:
        lines.append('⚠️ <i>Đang đọc IP kết nối trực tiếp (TRUST_PROXY_IP=0). Có thể là IP proxy Railway; xác minh trước khi chặn.</i>')
    return '\n'.join(lines)


def ip_overview(limit=15):
    """Compatibility wrapper used by existing commands."""
    return ip_dashboard('recent',0)


def ip_detail(ip):
    ip,ban,r=ip_action(ip)
    safe=html.escape(ip)
    status='⛔ ĐANG CHẶN' if ban else '🟢 CHƯA CHẶN'
    grant=ip_grant_status(ip)
    lines=['<b>◈ HTUNG / IP INSPECTOR</b>',
           '<code>SECURITY  •  IP DETAILS</code>',
           '━━━━━━━━━━━━━━━━━━━━',
           f'🌐 <code>{safe}</code>',
           f'🛡 Trạng thái: <b>{status}</b>',f'🔓 Quyền IP: <b>{fmt_ip_grant(grant)}</b>','']
    if r:
        when=lambda value: datetime.fromtimestamp(value,timezone(timedelta(hours=7))).strftime('%H:%M:%S · %d/%m/%Y')
        sec=int(r['active_seconds'] or 0)
        lines+= [f'📩 Yêu cầu API: <b>{int(r["requests"]):,}</b>',
                 f'⏳ Hoạt động ước tính: <b>{sec//60}p {sec%60:02d}s</b>',
                 f'🕐 Lần đầu: {when(r["first_seen"])}',
                 f'🕒 Lần cuối: {when(r["last_seen"])}',
                 f'🔗 Endpoint cuối: <code>{html.escape((r["last_path"] or "—")[:130])}</code>']
    else:
        lines.append('<i>IP này chưa có dữ liệu truy cập API.</i>')
    # UA/CPU are only browser-reported hints. An IP can be shared by many users.
    profile=parse_browser_profile(r.get('user_agent','') if r else '')
    threads=r.get('cpu_threads') if r else None
    lines+=['', '<b>▸ THIẾT BỊ / TRÌNH DUYỆT</b>',
            '📱 Loại: <b>'+html.escape(profile['device'])+'</b>',
            '💻 Hệ điều hành: <b>'+html.escape(profile['os'])+'</b>',
            '🌐 Trình duyệt: <b>'+html.escape(profile['browser'])+'</b>',
            '🧩 CPU: <b>Không thể xác định chip qua IP</b>',
            '⚙️ Luồng CPU trình duyệt báo: <b>'+str(int(threads))+'</b>' if threads else
                '⚙️ Luồng CPU trình duyệt báo: <i>Không có dữ liệu</i>']
    geo=geo_lookup(ip)
    lines+=['','<b>▸ VỊ TRÍ IP ƯỚC TÍNH</b>']
    if geo.get('note'):
        lines.append('📍 '+html.escape(geo['note']))
    else:
        place=', '.join(x for x in (geo['city'],geo['region'],geo['country']) if x)
        lines.append('📍 '+html.escape(place or 'Không có dữ liệu'))
        if geo['isp']:lines.append('📡 ISP: '+html.escape(geo['isp']))
        if geo['org'] and geo['org']!=geo['isp']:lines.append('🏢 Tổ chức: '+html.escape(geo['org']))
        if geo['asn']:lines.append('🛜 ASN: '+html.escape(geo['asn']))
        if geo['timezone']:lines.append('🕒 Múi giờ IP: '+html.escape(geo['timezone']))
    lines.append('<i>IP chỉ cho vị trí gần đúng; VPN/proxy làm sai lệch. UA có thể giả mạo; nhiều thiết bị có thể dùng chung IP.</i>')
    if ban:
        when=datetime.fromtimestamp(ban['banned_at'],timezone(timedelta(hours=7))).strftime('%H:%M · %d/%m/%Y')
        lines+=['',f'⛔ Đã chặn: {when}']
        if ban.get('reason'): lines.append('📝 Lý do: '+html.escape(str(ban['reason'])[:120]))
    lines += ['', '━━━━━━━━━━━━━━━━━━━━',
              '<i>Số phút chỉ ước lượng qua lượt gọi API, không phải thời gian chơi thực.</i>']
    if not TRUST_PROXY_IP:
        lines.append('⚠️ <i>Kiểm tra IP proxy trước khi khóa.</i>')
    return '\n'.join(lines)

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
    if board=='sunwin:hu':
        # V15: six diverse, auditable experts replace an ensemble of highly
        # correlated hypotheses for Sunwin ONLY. Every round remains predicted.
        sel=v15.choose(seq,v15.load_stats(con,board))
        final=sel['prediction']
        v15_rows=sel['model_scores']
        e={'agreement':sel['agreement'], 'strategy_count':len(v15.NAMES),
           'memory':None, 'engine':v15.VERSION,'regime':'AUDIT_ONLY',
           'edge':0.,'top':[{'name':nm,**details} for nm,details in v15_rows.items()]}
        caudecision={'champion':None,'prediction':final,'historical_score':.5,
                     'mode':'V15 · 6 TÍN HIỆU KIỂM TOÁN','sample_count':max(
                          (r['n'] for r in v15_rows.values()),default=0),
                     'baseline_score':.5,'top':[]}
    else:
        perf=perf_map(con,board);mem=memory_signal(con,board,seq)
        e=ensemble_prediction(seq,perf=perf,memory=mem,board=board,meta_history=meta_history)
        caulog=load_auto_cau_logs(con,board)
        caupred=acl.strategy_predictions(seq,board,meta_history,e['prediction'])
        caudecision=acl.select_predict(caupred,caulog,load_auto_champion(con,board),seq)
        final=caudecision['prediction']
    cauinfo=acl.detect_cau(seq)
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
    if board=='sunwin:hu' and d['base_key']:
        v15.seal_forecast(con,board,d['base_key'],v15.candidates(seq),d['updated_at'])
    # Preserve the original prediction sealed for this base round before a
    # restart/upgrade; otherwise the visible forecast and LIVE TXT can diverge.
    if board=='sunwin:hu' and d['base_key']:
        sealed=con.execute('SELECT prediction FROM issued_forecasts WHERE board=? AND base_key=?',
                           (board,d['base_key'])).fetchone()
        if sealed and sealed['prediction'] in ('TÀI','XỈU') and sealed['prediction']!=final:
            final=sealed['prediction']
            d['prediction']=final
            d['cau_mode']='FORECAST_LOCKED_BEFORE_UPGRADE'
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
        order='CAST(external_key AS INTEGER) DESC,id DESC' if board.startswith('sunwin:') else 'id DESC'
        latest_row=con.execute(f'SELECT external_key FROM rounds WHERE board=? ORDER BY {order} LIMIT 1',(board,)).fetchone()
        rows, guard_stats = guard.prepare(con,board,rows,source,now,str(latest_row['external_key']) if latest_row else None)
        with LOCK:
            LEARN_STATUS['rejected']+=guard_stats['rejected']
            LEARN_STATUS['stale_skipped']+=guard_stats['stale']
        if not rows:
            # Input was duplicated/stale. Do not reload history or run experts.
            return 0
        seq,meta_history=load_history_context(con,board)
        # prepare() already guarantees only new unique, validated keys.
        existing=set()
        last_row=con.execute(f'SELECT external_key,meta_json FROM rounds WHERE board=? ORDER BY {order} LIMIT 1',(board,)).fetchone()
        last_key=str(last_row['external_key']) if last_row else None
        try:last_meta=json.loads(last_row['meta_json'] or '{}') if last_row else {}
        except Exception:last_meta={}
        pending=[item for item in rows if item.get('result') in ('TÀI','XỈU') and str(item.get('key')) not in existing]
        if not pending:
            # Do not recompute 70+ legacy experts every polling cycle.
            return 0
        if board != 'sunwin:hu':
            bootstrap_auto_cau_history(con,board)
            perf=perf_map(con,board)
            auto_logs=load_auto_cau_logs(con,board)
            auto_champion=load_auto_champion(con,board)
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
            v15_decision=None
            if board=='sunwin:hu' and len(seq)>=12:
                # Choose now from PAST experts only, before inserting this outcome.
                v15_decision=v15.choose(seq,v15.load_stats(con,board))
                # Use ONLY immutable expert forecasts sealed before settlement.
                # Otherwise this is a causal REPLAY, not verified LIVE evidence.
                pre_v15=None
                if last_key is not None and acl.id_contiguous(last_key,key,last_meta,cur_meta,board):
                    pre_v15=v15.issued_forecast(con,board,last_key,now)
                v15.save_result(con,board,key,
                                pre_v15 if pre_v15 is not None else v15.candidates(seq),
                                actual,pre_v15 is not None,now)
            if len(seq)>=12 and do_eval:
                if board=='sunwin:hu':
                    # No redundant legacy 70+ experts: record exactly the
                    # prediction sealed BEFORE the outcome, when available.
                    issued=None
                    if last_key is not None and acl.id_contiguous(last_key,key,last_meta,cur_meta,board):
                        issued=con.execute('SELECT prediction,created_at FROM issued_forecasts '
                            'WHERE board=? AND base_key=?',(board,last_key)).fetchone()
                    is_live=bool(issued and float(issued['created_at'])<now
                                  and issued['prediction'] in ('TÀI','XỈU'))
                    final_pred=issued['prediction'] if is_live else v15_decision['prediction']
                    con.execute('''INSERT OR IGNORE INTO prediction_log
                    (board,external_key,prediction,confidence,actual,ok,agreement,memory_json,created_at,source)
                    VALUES(?,?,?,?,?,?,?,?,?,?)''',
                    (board,key,final_pred,50.,actual,int(final_pred==actual),
                    v15_decision['agreement']*100,None,now,'live' if is_live else 'replay'))
                    if is_live:
                        warmup.mark_forward_live(con,board,key,issued['created_at'],now)
                else:
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
    # The warmup gate is exclusive to Sunwin. Other boards are ready as soon
    # as there is an actual forecast; no new rounds need to be awaited.
    if board != 'sunwin:hu':
        return {'warmup_count': 0, 'warmup_target': 0,
                'warmup_remaining': 0, 'warmup_ready': True,
                'warmup_state': 'not_required'}
    with db() as con:
        return warmup.progress(con,board)


def upgrade_existing_history():
    """One-time causal replay of prior saved rounds on first new deployment.

    Does not alter pre-existing prediction_log or TXT records.
    """
    with db() as con:
        boards=['sunwin:hu'] if con.execute("SELECT 1 FROM rounds WHERE board='sunwin:hu' LIMIT 1").fetchone() else []
        for board in boards:
            seq,metas=load_history_context(con,board)
            if seq:create_current_prediction(con,board,seq,metas)
        con.commit()


def learn_payloads(sun_history=None,sun_current=None):
    total=0
    try:
        if sun_history is not None: total+=learn_rows('sunwin:hu',normalize_sun(sun_history),'sun_history')
        if sun_current is not None: total+=learn_rows('sunwin:hu',normalize_sun_current(sun_current),'sun_current')
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
            if r['board']=='sunwin:hu': LATEST[r['board']]=d


def summary_payload():
    with db() as con:
        rounds=int(con.execute("SELECT COUNT(*) FROM rounds WHERE board='sunwin:hu'").fetchone()[0]);boards=int(con.execute("SELECT COUNT(DISTINCT board) FROM rounds WHERE board='sunwin:hu'").fetchone()[0])
        patterns=int(con.execute("SELECT COUNT(*) FROM pattern_memory WHERE board='sunwin:hu'").fetchone()[0]);mature=int(con.execute("SELECT COUNT(*) FROM pattern_memory WHERE board='sunwin:hu' AND samples>=5").fetchone()[0])
        sample=int(con.execute("SELECT COALESCE(SUM(samples),0) FROM pattern_memory WHERE board='sunwin:hu'").fetchone()[0]);pr=con.execute("SELECT COUNT(*) n,COALESCE(SUM(ok),0) w FROM prediction_log WHERE board='sunwin:hu'").fetchone()
        sn=int(pr['n'] or 0);sw=int(pr['w'] or 0);strategies=int(con.execute("SELECT COUNT(DISTINCT strategy) FROM strategy_log WHERE board='sunwin:hu'").fetchone()[0])
        lv=con.execute("SELECT COUNT(*) n,COALESCE(SUM(ok),0) w FROM prediction_log WHERE board='sunwin:hu' AND source='live'").fetchone()
        ln=int(lv['n'] or 0);lw=int(lv['w'] or 0)
    with db() as con:
        ac=con.execute("SELECT COUNT(*) n,COUNT(DISTINCT strategy) strategies,COALESCE(SUM(CASE WHEN source='backfill' THEN 1 ELSE 0 END),0) backfill FROM auto_cau_log WHERE board='sunwin:hu'").fetchone()
    with LOCK:st=dict(LEARN_STATUS)
    return {'ok':True,'auto_cau_logs':int(ac['n'] or 0),'auto_cau_strategies':int(ac['strategies'] or 0),'auto_cau_backfill':int(ac['backfill'] or 0),'rounds':rounds,'boards':boards,'patterns':patterns,'mature_patterns':mature,'pattern_samples':sample,
            'settled_predictions':sn,'wins':sw,'win_rate':round(sw/sn*100,2) if sn else None,
            'live_predictions':ln,'live_wins':lw,'live_win_rate':round(lw/ln*100,2) if ln else None,
            'replay_predictions':sn-ln,'strategies':strategies,
            'engine':'SUNWIN V15 · 6 nhóm + MD5/SHA256 Railway','learner':st}


def board_rows_payload():
    with db() as con:
        rows=con.execute('''
          SELECT r.board,COUNT(*) rounds,
                 (SELECT COUNT(*) FROM pattern_memory p WHERE p.board=r.board) patterns,
                 (SELECT COUNT(*) FROM pattern_memory p WHERE p.board=r.board AND p.samples>=5) mature,
                 (SELECT COUNT(*) FROM prediction_log q WHERE q.board=r.board) pn,
                 (SELECT COALESCE(SUM(ok),0) FROM prediction_log q WHERE q.board=r.board) pw
          FROM rounds r WHERE r.board='sunwin:hu' GROUP BY r.board ORDER BY rounds DESC
        ''').fetchall()
    out=[]
    for r in rows:
        n=int(r['pn'] or 0);w=int(r['pw'] or 0)
        out.append({'board':r['board'],'rounds':int(r['rounds']),'patterns':int(r['patterns'] or 0),'mature':int(r['mature'] or 0),
                    'predictions':n,'wins':w,'win_rate':round(w/n*100,1) if n else None})
    return out


def top_algo_payload(board=None,limit=20):
    board='sunwin:hu'  # V16 only ranks Sunwin, never legacy Baccarat
    where='WHERE board=?';params=[board]
    with db() as con:
        rows=con.execute(f'''SELECT strategy,COUNT(*) n,SUM(ok) wins FROM strategy_log {where}
                            GROUP BY strategy HAVING COUNT(*)>=8
                            ORDER BY (SUM(ok)+8.0)/(COUNT(*)+16.0) DESC,COUNT(*) DESC LIMIT ?''',params+[int(limit)]).fetchall()
    return [{'strategy':r['strategy'],'n':int(r['n']),'wins':int(r['wins']),'rate':round(int(r['wins'])/int(r['n'])*100,1),
             'bayes':round((int(r['wins'])+8)/(int(r['n'])+16)*100,1)} for r in rows]


def prediction_payload(board):
    # Never stop backend forecasts. Only Sunwin hides the public prediction
    # during its first 30 settled LIVE forecasts of this deployment.
    gate=warmup_for(board)
    with LOCK:d=dict(LATEST.get(board) or {})
    if not d:
        with db() as con:r=con.execute('SELECT details_json FROM current_prediction WHERE board=?',(board,)).fetchone()
        if r:
            try:d=json.loads(r['details_json'])
            except Exception:d={}
    if not d:
        return {'ok':True,'board':board,'prediction':None,'display':'ĐANG ĐỒNG BỘ',
                'signal_status':'warming',
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
        for key in ('prediction','display','top','cau_top'):
            out.pop(key,None)
        out.update({'prediction':None,'display':f'KHỞI ĐỘNG {gate["warmup_count"]}/{gate["warmup_target"]}'})
        return out
    out['display']='TÀI' if d.get('prediction')=='TÀI' else 'XỈU'
    return out



# MD5/SHA256 remains server-side: the HTML only transports input and displays
# results. Every record is isolated to its anonymous browser token fingerprint.
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

# V22: public static mode can read only settled Sunwin rounds + game URL.
# Prediction endpoints, raw cache, warmup and admin APIs require device/IP approval.
# IP bans continue to override any grant.
PUBLIC_SUNWIN_ENDPOINTS = frozenset({'/api/public/sunwin/rounds','/api/public/sunwin/config'})

def static_public_rounds(limit=260):
    """Settled results only. No private prediction, strategy weights or tokens."""
    with db() as con:
        rows=con.execute('''SELECT external_key,result,meta_json,seen_at FROM rounds
            WHERE board='sunwin:hu' ORDER BY CAST(external_key AS INTEGER) DESC,id DESC LIMIT ?''',
            (min(300,max(1,int(limit))),)).fetchall()
    results=[]
    for r in reversed(rows):
        result=r['result']
        if result not in ('TÀI','XỈU'):continue
        try:meta=json.loads(r['meta_json'] or '{}')
        except (ValueError,TypeError):meta={}
        total=meta.get('total')
        try:total=int(total) if total is not None else None
        except (TypeError,ValueError):total=None
        results.append({'session':str(r['external_key']),'result':result,
                        'total':total,'seen_at':round(float(r['seen_at'] or 0))})
    return {'ok':True,'source':'SETTLED_HISTORY_ONLY','rounds':results,
            'updated_at':results[-1]['seen_at'] if results else None}

class Handler(BaseHTTPRequestHandler):
    server_version='ThanhtungLearning/4.0-OMNIMAX'
    def log_message(self,fmt,*args): return
    def send_bytes(self,code,body,ctype='application/json; charset=utf-8'):
        # Safari/mobile and reverse proxies often disconnect before a request finishes.
        # This is normal client behaviour, not a worker crash. Avoid huge tracebacks.
        try:
            self.send_response(code)
            self.send_header('Content-Type',ctype)
            self.send_header('Content-Length',str(len(body)))
            self.send_header('Cache-Control','no-store, no-cache, must-revalidate')
            self.send_header('Access-Control-Allow-Origin','*')
            self.send_header('Access-Control-Allow-Methods','GET, POST, OPTIONS')
            self.send_header('Access-Control-Allow-Headers','Content-Type, X-Access-Token, X-Client-Threads')
            self.send_header('X-Content-Type-Options','nosniff')
            self.end_headers()
            if body:self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, TimeoutError):
            self.close_connection=True
            return
        except OSError as exc:
            if getattr(exc,'errno',None) in (32, 104, 103):  # EPIPE / ECONNRESET / ECONNABORTED
                self.close_connection=True
                return
            raise
    def do_OPTIONS(self): self.send_bytes(204,b'','text/plain')
    def do_POST(self):
        path=urlparse(self.path).path.rstrip('/') or '/'
        permitted={'/access/register','/api/md5/analyze','/api/md5/learn','/api/md5/import'}
        if path not in permitted:
            return self.send_bytes(404,json_bytes({'ok':False,'error':'not found'}))
        try:
            ip=visitor_ip(self)
            token=self.headers.get('X-Access-Token','')
            public_md5=path in ('/api/md5/analyze','/api/md5/learn','/api/md5/import')
            if public_md5:
                # V20: MD5 is FREE. No access-register, admin approval, IP grant,
                # IP ban or device entitlement check on these three endpoints.
                # The optional token is ONLY a browser-scoped data namespace.
                fingerprint=access_fingerprint(token)
                if not fingerprint:
                    # Legacy clients without a token remain usable; their personal
                    # learning data is scoped to the IP+UA combination, not global.
                    anon=(ip+'\0'+self.headers.get('User-Agent','')[:160]).encode('utf-8','replace')
                    fingerprint=hashlib.sha256(b'md5-v20-public\0'+anon).hexdigest()
                # Rate limits protect the public endpoint from accidental overload.
                # The wider IP window allows legitimate shared connections.
                if not (md5_rate_allowed('md5-browser:'+fingerprint,30) and
                        md5_rate_allowed('md5-visitor:'+ip,240)):
                    return self.send_bytes(429,json_bytes({'ok':False,'error':'Quá nhiều yêu cầu MD5. Vui lòng thử lại sau.'}))
            else:
                # Sunwin / admin access continues to require approval and IP checks.
                if not record_visit(ip,path,self.headers.get('User-Agent',''),self.headers.get('X-Client-Threads','')):
                    return self.send_bytes(403,json_bytes(blocked_ip_payload(ip)))
            size=int(self.headers.get('Content-Length','0'))
            max_size=16384 if path=='/api/md5/import' else 2048
            if size<=0 or size>max_size:
                return self.send_bytes(413,json_bytes({'ok':False,'error':'Invalid payload size'}))
            payload=json.loads(self.rfile.read(size))
            if not isinstance(payload,dict):
                raise ValueError('Payload không hợp lệ')
            if path=='/access/register':result=access_register(payload.get('token'),ip)
            elif path=='/api/md5/analyze':
                result=md5_analyze(fingerprint,payload.get('hash'))
                # MD5 is independent: serve each requested hash analysis
                # immediately after normal access checks. Keep warmup_ready
                # for backward compatibility with the existing V13 HTML.
                result.update({'warmup_ready':True,'warmup_count':0,
                               'warmup_target':0,'warmup_remaining':0,
                               'warmup_state':'not_required',
                               'signal_status':'predict'})

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
            if not record_visit(ip,path,self.headers.get('User-Agent',''),self.headers.get('X-Client-Threads','')):
                return self.send_bytes(403,json_bytes(blocked_ip_payload(ip)))
        except (ValueError,sqlite3.Error):
            return self.send_bytes(503,json_bytes({'ok':False,'error':'Access control unavailable'}))
        if path=='/access/status':
            status=access_status(self.headers.get('X-Access-Token',''),ip)
            return self.send_bytes(200 if status.get('ok') else 401,json_bytes(status))
        if (path=='/config' or path.startswith('/api/')) and path not in PUBLIC_SUNWIN_ENDPOINTS:
            # V22: only settled-round public feed is free; predictions require approval.
            status=access_status(self.headers.get('X-Access-Token',''),ip)
            if not status.get('allowed'):
                return self.send_bytes(403,json_bytes({'ok':False,'error':'Chua duoc cap quyen','status':status.get('status')}))
        if path=='/':return self.send_bytes(200,json_bytes({'ok':True,'service':'DEVELOPER THANHTUNG BACKEND','mode':'Sunwin 24/7 + MD5/SHA256','engine':'SUNWIN V15 · 6 nhóm','web':'host index externally'}))
        if path=='/api/public/sunwin/config':
            return self.send_bytes(200,json_bytes({'ok':True,'sunwin_game_url':public_config().get('sunwin_game_url','')}))
        if path=='/api/public/sunwin/rounds':
            return self.send_bytes(200,json_bytes(static_public_rounds()))
        if path=='/config':return self.send_bytes(200,json_bytes({'ok':True,**public_config()}))
        if path=='/api/sun/current':
            c=cache_public('sun_current');return self.send_bytes(200 if c.get('ok') else 503,json_bytes(c))
        if path=='/api/sun/history':
            c=cache_public('sun_history');return self.send_bytes(200 if c.get('ok') else 503,json_bytes(c))
        if path=='/api/warmup':
            board=(qs.get('board') or ['sunwin:hu'])[0]
            # Only expose board progress; no predictions prior to readiness.
            if board!='sunwin:hu':
                return self.send_bytes(400,json_bytes({'ok':False,'error':'Invalid board'}))
            return self.send_bytes(200,json_bytes({'ok':True,'board':board,**warmup_for(board)}))
        if path=='/api/learning/summary':return self.send_bytes(200,json_bytes(summary_payload()))
        if path=='/api/learning/boards':return self.send_bytes(200,json_bytes({'ok':True,'boards':board_rows_payload()}))
        if path=='/api/learning/top':
            board=(qs.get('board') or [None])[0];return self.send_bytes(200,json_bytes({'ok':True,'board':board,'rows':top_algo_payload(board,30)}))
        if path=='/api/prediction/sunwin':return self.send_bytes(200,json_bytes(prediction_payload('sunwin:hu')))
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
        next_hist=0.0
        while True:
            cfg=public_config();now=time.monotonic();sun_data=hist_data=None
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
            if sun_data is not None or hist_data is not None:
                await asyncio.to_thread(learn_payloads,hist_data,sun_data)
            await asyncio.sleep(.55)



EXPORT_TIMEZONE = timezone(timedelta(hours=7))


def _txt_time(timestamp):
    try:
        return datetime.fromtimestamp(float(timestamp), EXPORT_TIMEZONE).strftime('%H:%M:%S · %d/%m/%Y')
    except (ValueError, TypeError, OverflowError, OSError):
        return '—'


def _txt_side(board, value):
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


def export_history_txt(kind='sun', limit=None, include_unpredicted=False):
    """Export settled predictions, or (opt-in) all observed game results.

    Historical rounds without a prediction remain explicitly *unpredicted*;
    they are never counted as correct/incorrect. No retrospective predictions
    are invented to fill a reset/replaced SQLite database.
    """
    if kind != 'sun':
        raise ValueError('Loại lịch sử không hợp lệ')
    if limit is not None:
        limit = int(limit)
        if not 1 <= limit <= 100000:
            raise ValueError('Số phiên phải từ 1 đến 100000')
    board_filter = "r.board='sunwin:hu'"
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
                ORDER BY {'CAST(r.external_key AS INTEGER) DESC,r.id DESC'}"""
    else:
        pred_filter = "p.board='sunwin:hu'"
        sql=f"""SELECT p.board,p.external_key,p.prediction,p.actual,p.ok,
                       p.created_at,r.meta_json,r.seen_at,p.source
                FROM prediction_log p LEFT JOIN rounds r
                ON r.board=p.board AND r.external_key=p.external_key
                WHERE {pred_filter} AND p.actual IN ('TÀI','XỈU')
                ORDER BY {'CAST(p.external_key AS INTEGER) DESC'}"""
    if limit is not None:
        sql+=' LIMIT ?';params.append(limit)
    with db() as con:
        entries=[dict(x) for x in con.execute(sql,params)]
        seen_total=con.execute('SELECT COUNT(*) FROM rounds WHERE '+board_filter.replace('r.board','board'),()).fetchone()[0]
    entries.reverse()
    title='SUNWIN'
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
            lines.extend(['','SUNWIN · TÀI/XỈU','-'*34]);last_board=board
        try:
            meta=json.loads(r['meta_json'] or '{}')
            if not isinstance(meta,dict):meta={}
        except (json.JSONDecodeError,TypeError):meta={}
        identifier=meta.get('sid') or r['external_key']
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


def ip_keyboard(mode='recent',page=0):
    """Short callback labels, no clutter from per-IP long text buttons."""
    def b(label,key):return {'text':label,'callback_data':'adm:'+key}
    mode=mode if mode in ('blocked','granted') else 'recent'
    page=max(0,min(int(page),9999))
    with db() as con:
        if mode=='granted':count=con.execute('SELECT COUNT(*) FROM ip_grants WHERE expires_at>?',(time.time(),)).fetchone()[0]
        else:count=con.execute('SELECT COUNT(*) FROM '+('banned_ips' if mode=='blocked' else 'visitor_ips')).fetchone()[0]
        maximum=max(0,(count-1)//IP_PAGE_SIZE)
        page=min(page,maximum)
        if mode=='granted':
            items=con.execute('SELECT ip FROM ip_grants WHERE expires_at>? ORDER BY expires_at ASC LIMIT ? OFFSET ?',
                              (time.time(),IP_PAGE_SIZE,IP_PAGE_SIZE*page)).fetchall()
        elif mode=='blocked':
            items=con.execute('SELECT ip FROM banned_ips ORDER BY banned_at DESC LIMIT ? OFFSET ?',
                              (IP_PAGE_SIZE,IP_PAGE_SIZE*page)).fetchall()
        else:
            items=con.execute('SELECT ip FROM visitor_ips ORDER BY last_seen DESC LIMIT ? OFFSET ?',
                              (IP_PAGE_SIZE,IP_PAGE_SIZE*page)).fetchall()
    rows=[[b('🟢 GẦN ĐÂY','ips:recent:0'),b('🔓 ĐƯỢC CẤP','ips:granted:0'),b('⛔ ĐÃ CHẶN','ips:blocked:0')]]
    for i,r in enumerate(items,1+page*IP_PAGE_SIZE):
        addr=r['ip']
        label=f'🔎 {i:02d} · {addr}'
        if len(('adm:ipview:'+addr).encode())<=64:
            rows.append([b(label,'ipview:'+addr)])
    navigation=[]
    if page>0:navigation.append(b('◀ TRƯỚC',f'ips:{mode}:{page-1}'))
    navigation.append(b(f'{page+1}/{maximum+1}',f'ips:{mode}:{page}'))
    if page<maximum:navigation.append(b('SAU ▶',f'ips:{mode}:{page+1}'))
    rows.append(navigation)
    rows.append([b('🔄 CẬP NHẬT',f'ips:{mode}:{page}'),b('🏠 MENU','home')])
    return {'inline_keyboard':rows}


def admin_keyboard(page='home'):
    """V17: minimal two-game admin console and explicit IP inspector."""
    def b(label, key):return {'text':label,'callback_data':'adm:'+key}
    if page=='ips':return ip_keyboard('recent',0)
    if page.startswith('ips:'):
        parts=page.split(':')
        try:return ip_keyboard(parts[1],int(parts[2]))
        except (IndexError,ValueError):return ip_keyboard()
    if page.startswith('ipview:'):
        ip=page[7:]
        try:
            _,ban,_=ip_action(ip)
            action='unban' if ban else 'ban'
            title='✅ MỞ CHẶN' if ban else '⛔ CHẶN IP'
            grant=ip_grant_status(ip)
            rows=[]
            if not ban:
                rows.append([b('⏱ 2 GIỜ',f'ipgrant:2h:{ip}'),b('📅 2 NGÀY',f'ipgrant:2d:{ip}'),b('🗓 7 NGÀY',f'ipgrant:7d:{ip}')])
                if grant:rows.append([b('🔒 THU HỒI QUYỀN IP',f'iprevoke:{ip}')])
            rows.append([b(title,f'ipconfirm:{action}:{ip}')])
            rows.append([b('🌐 DANH SÁCH IP','ips:recent:0'),b('🏠 MENU','home')])
            return {'inline_keyboard':rows}
        except ValueError:return ip_keyboard()
    if page.startswith('ipgrant:'):
        _,duration,ip=page.split(':',2)
        return {'inline_keyboard':[[b('✅ CẤP QUYỀN',f'ipgrantdo:{duration}:{ip}'),
                                    b('↩ HỦY',f'ipview:{ip}')]]}
    if page.startswith('ipconfirm:'):
        _,action,ip=page.split(':',2)
        return {'inline_keyboard':[[b('✅ XÁC NHẬN',f'ipdo:{action}:{ip}'),
                                    b('↩ HỦY',f'ipview:{ip}')],
                                   [b('🏠 MENU','home')]]}
    menus={
        'home': [
            [b('🎲 SUNWIN','boards'),b('🔐 MD5 / SHA256','md5stats')],
            [b('📊 LIVE / KIỂM ĐỊNH','overview'),b('🤖 AUTO 30','warmup')],
            [b('🌐 IP / CẤP THỜI HẠN','ips:recent:0'),b('🔑 DUYỆT THIẾT BỊ','access')],
            [b('🩺 HỆ THỐNG','system'),b('⚙️ CÀI ĐẶT','settings')],
            [b('📥 XUẤT TXT','export'),b('❔ HƯỚNG DẪN','help')],
            [b('↻ LÀM MỚI','home')],
        ],
        'access':[[b('↻ TẢI YÊU CẦU','access')]],
        'warmup':[[b('↻ AUTO 30','warmup')]],
        'v15':[[b('↻ KIỂM TRA V15','v15'),b('🤖 AUTO 30','warmup')]],
        'overview':[[b('🎯 LIVE','live'),b('📐 KIỂM ĐỊNH','quant')],
                    [b('📊 HỌC CẦU','stats'),b('🧪 V15','v15')],
                    [b('📉 ĐỘ LỆCH','bias'),b('🔐 MD5','md5stats')]],
        'system':[[b('📡 API','api'),b('🗃 SQLITE','dbstatus')],
                  [b('🧯 KIỂM TOÁN','auditdata'),b('📉 ĐỘ LỆCH','bias')]],
        'settings':[[b('🔗 LINK / API','links'),b('🩺 KIỂM TRA','api')],
                    [b('❔ HƯỚNG DẪN','settingshelp')]],
        'export':[[b('📄 SUNWIN · LIVE','exportsun')],
                  [b('📚 SUNWIN · TOÀN BỘ','exportsunall')]],
        'live':[[b('📐 KIỂM ĐỊNH','quant'),b('🧪 V15','v15')]],
        'quant':[[b('🎯 LIVE','live'),b('📊 HỌC CẦU','stats')]],
        'stats':[[b('🧪 V15','v15'),b('🎲 SUNWIN','boards')]],
        'algo':[[b('🧪 V15','v15'),b('📐 KIỂM ĐỊNH','quant')]],
        'boards':[[b('🎯 LIVE','live'),b('🔐 MD5','md5stats')]],
        'md5stats':[[b('↻ THỐNG KÊ MD5','md5stats'),b('🎲 SUNWIN','boards')]],
        'api':[[b('🗃 SQLITE','dbstatus'),b('⚙️ LINK / API','links')]],
        'dbstatus':[[b('📡 API','api'),b('🧯 KIỂM TOÁN','auditdata')]],
        'auditdata':[[b('🗃 SQLITE','dbstatus'),b('📉 ĐỘ LỆCH','bias')]],
        'bias':[[b('🎯 LIVE','live'),b('📐 KIỂM ĐỊNH','quant')]],
        'links':[[b('⚙️ CÀI ĐẶT','settings'),b('❔ CÁCH SỬA','settingshelp')]],
        'help':[[b('🌐 QUẢN LÝ IP','ips:recent:0'),b('⚙️ CÀI ĐẶT','settings')]],
        'iphelp':[[b('🌐 DANH SÁCH IP','ips:recent:0')]],
        'settingshelp':[[b('🔗 LINK / API','links')]],
        'health':[[b('📡 API','api'),b('🗃 SQLITE','dbstatus')]],
    }
    rows=list(menus.get(page,menus['home']))
    if page!='home':
        if page not in ('settings','export','overview','system'):
            rows.append([b('↻ CẬP NHẬT',page)])
        rows.append([b('🏠 TRANG CHỦ','home')])
    return {'inline_keyboard':rows}


def admin_home():
    s=summary_payload();now=time.time()
    with LOCK:
        apis={k:dict(v) for k,v in CACHE.items()}
        state=dict(LEARN_STATUS)
    fresh=sum(bool(v.get('ok') and now-v.get('ts',0)<120) for v in apis.values())
    uptime=max(0,int(now-state.get('started',now)))
    live='—' if s['live_win_rate'] is None else f'{s["live_win_rate"]}%'
    with db() as con:
        banned=con.execute('SELECT COUNT(*) FROM banned_ips').fetchone()[0]
        active_grants=con.execute('SELECT COUNT(*) FROM ip_grants WHERE expires_at>?',(now,)).fetchone()[0]
        pending=con.execute("SELECT COUNT(*) FROM access_requests WHERE status='pending'").fetchone()[0]
    stamp=datetime.fromtimestamp(now,timezone(timedelta(hours=7))).strftime('%H:%M · %d/%m/%Y')
    return ('<b>◈ HTUNG / CONTROL CENTER</b>\n'
            '<code>SUNWIN  •  MD5  •  RAILWAY</code>\n'
            '━━━━━━━━━━━━━━━━━━━━\n'
            f'📡 API Sunwin    {"🟢" if fresh==2 else "🟡" if fresh else "🔴"} <b>{fresh}/2</b>\n'
            f'⚡ Worker        <b>{uptime//3600}h {(uptime%3600)//60}m</b>\n'
            f'🤖 Auto Sunwin   <b>{warmup_for("sunwin:hu")["warmup_count"]}/30</b>\n'
            '━━━━━━━━━━━━━━━━━━━━\n'
            f'🎯 LIVE          <b>{s["live_predictions"]:,}</b> · <b>{live}</b>\n'
            f'📚 Phiên Sunwin   <b>{s["rounds"]:,}</b>\n'
            f'🌐 IP đang chặn  <b>{banned}</b>\n'
            f'🔑 Chờ duyệt      <b>{pending}</b>\n'
            '━━━━━━━━━━━━━━━━━━━━\n'
            f'<i>🕒 {stamp} GMT+7</i>\n'
            '<i>Chọn tác vụ bên dưới. Dữ liệu LIVE không bảo đảm dự đoán phiên tới.</i>')

def admin_overview_text():
    return ('<b>📊 TRUNG TÂM THỐNG KÊ</b>\n━━━━━━━━━━━━━━━━━━━━\n'
            '🎯 LIVE: dữ liệu dự đoán thực trước khi chốt phiên.\n'
            '📐 Kiểm định: thống kê 20–200 phiên LIVE.\n'
            '📊 Học cầu: số mẫu và trạng thái worker.\n'
            '🏆 Thuật toán: kết quả kiểm tra có hiệu chỉnh.\n'
            '📉 Lệch: phát hiện thiên lệch chọn TÀI/XỈU.\n'
            '🎲 Sunwin: tình trạng phiên và LIVE.\n'
            '🔐 MD5: thống kê phân tích theo yêu cầu.\n\n'
            '<i>Chọn chức năng để xem số liệu thực từ SQLite.</i>')


def md5_stats_text():
    """MD5 usage overview without exposing approved device fingerprints or hashes."""
    with db() as con:
        scanned=int(con.execute('SELECT COUNT(*) FROM md5_scans').fetchone()[0])
        labels=int(con.execute('SELECT COUNT(*) FROM md5_labels').fetchone()[0])
        users=int(con.execute('SELECT COUNT(DISTINCT token_hash) FROM md5_labels').fetchone()[0])
        checked=con.execute("SELECT COUNT(*) n,COALESCE(SUM(prior_prediction=actual),0) w "
                            "FROM md5_labels WHERE prior_prediction IN ('TÀI','XỈU')").fetchone()
    n=int(checked['n'] or 0);w=int(checked['w'] or 0)
    return ('<b>🔐 MD5 / SHA256 · RAILWAY</b>\n━━━━━━━━━━━━━━━━━━━━\n'
            f'🧩 Phân tích chưa gắn nhãn: <b>{scanned:,}</b>\n'
            f'📚 Nhãn kết quả: <b>{labels:,}</b>\n'
            f'👤 Thiết bị có lịch sử nhãn: <b>{users:,}</b>\n'
            f'🎯 Nhãn có dự đoán trước: <b>{w}/{n}</b>'
            + (f' ({100*w/n:.1f}%)' if n else '')
            + '\n\n<i>MD5 phân tích theo yêu cầu, không chạy phiên tự động. '
              'Nhãn lưu bởi người dùng, không được xác minh độc lập. '
              'Điểm hash không phải xác suất thắng.</i>')


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
            '🔐 MD5 / SHA256: ✅ API Railway (xử lý theo yêu cầu)\n'
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
    return ('<b>◈ HTUNG / IP CONTROL</b>\n'
            '━━━━━━━━━━━━━━━━━━━━\n'
            '🔎 <code>/ip 1.2.3.4</code> · Xem chi tiết\n'
            '⛔ <code>/banip 1.2.3.4</code> · Chặn\n'
            '✅ <code>/unbanip 1.2.3.4</code> · Mở chặn\n'
            '🔓 <code>/capip 1.2.3.4 2h</code> · Cấp 2 giờ\n'
            '📅 <code>/capip 1.2.3.4 2d</code> · Cấp 2 ngày\n'
            '🔒 <code>/thuhoip 1.2.3.4</code> · Thu hồi\n\n'
            '<i>Trong menu IP, chạm địa chỉ để xem, sau đó xác nhận khi chặn hoặc mở. '
            'TRUST_PROXY_IP=0 có thể ghi IP proxy Railway. Vị trí/ISP cần IP_GEO_ENABLED=1 (gọi ipwho.is); hệ điều hành/trình duyệt suy từ UA, CPU chỉ là số luồng nếu có.</i>')

def admin_settings_help_text():
    return ('<b>⚙️ LỆNH THIẾT LẬP</b>\n━━━━━━━━━━━━━━━━━━━━\n'
            '🎮 Sunwin: <code>/setsunlink URL</code>\n'
            '📡 API current: <code>/setsunapi URL</code>\n'
            '📜 API history: <code>/setsunhistory URL</code>\n'
            '🛰 API fallback: <code>/setsunfallback URL</code>\n'
            
            '🔄 Khôi phục mặc định: <code>/resetlinks</code>\n\n'
            '<i>Chỉ sử dụng /resetlinks khi chắc chắn muốn bỏ các URL đã chỉnh.</i>')


def admin_help_text():
    return ('<b>❔ HƯỚNG DẪN BOT ADMIN</b>\n━━━━━━━━━━━━━━━━━━━━\n'
            '🏠 <code>/start</code> hoặc <code>/menu</code> · Bảng điều khiển\n'
            '📊 <code>/stats</code> · Thống kê học nền\n'
            '🔐 <code>/md5stats</code> · Thống kê MD5/SHA256\n'
            '🎯 <code>/live</code> · Kiểm định LIVE\n'
            '🩺 <code>/diagnose</code> · Chẩn đoán\n'
            '🌐 <code>/ips</code> · IP: chặn/mở, cấp quyền 2h/2d/7d\n'
            '⏱ <code>/capip IP 12h</code> · Tự chọn thời hạn\n'
            '⚙️ <code>/links</code> · Link/API\n'
            '📥 <code>/exportmenu</code> · Xuất TXT\n'
            '\n'
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
            '📥 TXT SUNWIN: chỉ phiên <b>có dự đoán</b>.\n'
            '📚 TOÀN BỘ: cả phiên không dự đoán.\n\n'
            'Dùng <code>/exportsun 500</code> để giới hạn số phiên.\n<i>Không tạo dữ liệu hoặc dự đoán giả.</i>')


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
    return '<b>📡 API STATUS</b>\n'+one('sun_current','SUN CURRENT')+'\n'+one('sun_history','SUN HISTORY')


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
        d=guard.diagnostics(con,'sunwin:hu')
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


def v15_admin_text():
    with db() as con:
        records=v15.audit(con,'sunwin:hu')
        pred=con.execute("SELECT COUNT(*) n,COALESCE(SUM(ok),0) w FROM prediction_log WHERE board='sunwin:hu' AND source='live'").fetchone()
        recent=con.execute("SELECT ok FROM prediction_log WHERE board='sunwin:hu' AND source='live' ORDER BY created_at DESC LIMIT 100").fetchall()
    live=records.get('live',{}); replay=records.get('replay',{})
    n=int(pred['n'] or 0);w=int(pred['w'] or 0)
    rn=len(recent);rw=sum(int(r[0]) for r in recent)
    return ('🧪 <b>SUNWIN V15 · KIỂM TOÁN</b>\n'
            f'• Bộ máy: 6 nhóm tín hiệu không bỏ phiên\n'
            f'• Đã đối chiếu LIVE (mọi đời): {w}/{n} ({round(w/n*100,2) if n else 0}%)\n'
            f'• 100 LIVE gần nhất: {rw}/{rn} ({round(rw/rn*100,2) if rn else 0}%)\n'
            f'• V15 expert LIVE đã ghi: {int(live.get("rounds",0))} phiên\n'
            f'• V15 expert REPLAY (không tính LIVE): {int(replay.get("rounds",0))} phiên\n'
            '• Điểm mô hình không phải xác suất thắng.\n'
            '• Khởi động hiển thị Sunwin vẫn 30 phiên LIVE / Deploy.')


def warmup_admin_text():
    sun=warmup_for('sunwin:hu')
    lines=['<b>🤖 AUTO 24/24 · SUNWIN CHẠY NGẦM 30 PHIÊN</b>',
           '━━━━━━━━━━━━━━━━━━━━',
           f'🎲 SUNWIN: <b>{sun["warmup_count"]}/{sun["warmup_target"]}</b> · '+('✅ ĐÃ HIỆN' if sun['warmup_ready'] else '⏳ ĐANG CHẠY NGẦM'),
           '🔐 MD5/SHA256: Hoạt động theo yêu cầu, không có warmup',
           '\n<i>Chỉ đếm 30 dự đoán Sunwin LIVE đã phát trước kết quả. API vẫn tạo dự đoán trong giai đoạn chạy ngầm. Đây không phải cam kết tỷ lệ đúng.</i>']
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
        rate='—' if r['win_rate'] is None else f"{r['win_rate']}%";name=html.escape(r['board'])
        
        with LOCK: lp=dict(LATEST.get(r['board']) or {})
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
            '<code>/setsunlink URL</code>\n<code>/setsunapi URL</code>\n<code>/setsunhistory URL</code>\n<code>/setsunfallback URL</code>')




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
    try:
        if cmd in ('/start','/menu'):out=admin_home()
        elif cmd in ('/panel','/overview'):out=admin_overview_text()
        elif cmd in ('/help','/huongdan'):out=admin_help_text()
        elif cmd=='/settings':out=admin_settings_text()
        elif cmd=='/system':out=admin_system_text()
        elif cmd in ('/status','/api'):out=api_status_text()
        elif cmd in ('/stats','/learn'):out=stats_text()
        elif cmd in ('/warmup','/auto30'):out=warmup_admin_text()
        elif cmd in ('/v15','/auditv15'):out=v15_admin_text()
        elif cmd in ('/live','/liveaudit'):out=v10_live_report()
        elif cmd in ('/loc','/qr','/taoma','/createcode','/taokey'):
            out='ℹ️ Chức năng này đã được bỏ khỏi bot admin.'
        elif cmd in ('/quant','/modelcheck'):out=quant_report()
        elif cmd in ('/yeucau','/quyenweb'):out=access_pending_text()
        elif cmd in ('/capquyen','/thuhoi'):
            out=access_set(arg,cmd=='/capquyen')
        elif cmd=='/ips':out=ip_overview()
        elif cmd=='/ip':
            if not arg:raise ValueError('Dùng /ip 1.2.3.4')
            out=ip_detail(arg)
        elif cmd=='/capip':
            params=arg.split()
            if len(params)!=2:raise ValueError('Dùng /capip 1.2.3.4 2h (hoặc 2d, 7d, 30m, 48h).')
            ip,grant=ip_grant_action(params[0],'grant',params[1],uid)
            out=f'✅ CẤP QUYỀN IP\n<code>{html.escape(ip)}</code>\n{fmt_ip_grant(grant)}'
        elif cmd=='/thuhoip':
            if not arg:raise ValueError('Dùng /thuhoip 1.2.3.4')
            ip,_=ip_grant_action(arg.split()[0],'revoke')
            out=f'🔒 Đã thu hồi quyền IP <code>{html.escape(ip)}</code>'
        elif cmd in ('/banip','/unbanip'):
            if not arg:raise ValueError('Dùng /banip 1.2.3.4')
            target=arg.split()[0]
            ip,ban,info=ip_action(target,'ban' if cmd=='/banip' else 'unban',
                                  ' '.join(arg.split()[1:]))
            out=('⛔ Đã chặn ' if cmd=='/banip' else '✅ Đã mở ')+f'<code>{html.escape(ip)}</code>'
        elif cmd=='/boards':out=boards_text()
        elif cmd=='/md5stats':out=md5_stats_text()
        elif cmd in ('/topalgo','/algo'):out=algo_text()
        elif cmd=='/links':out=links_text()
        elif cmd=='/dbstatus':out=db_status_text()
        elif cmd in ('/auditdata','/dataaudit'):out=data_audit_text()
        elif cmd in ('/bias','/modelhealth'):out=model_health_text()
        elif cmd=='/diagnose':out=admin_system_text()
        elif cmd=='/exportmenu':out=export_menu_text()
        elif cmd in ('/exportsun','/txtsun','/exportsunall'):
            kind = 'sun'
            include_all=cmd=='/exportsunall'
            limit = _parse_txt_limit(arg)
            count = await send_txt_document(client, chat_id, kind, limit, include_all)
            out = f'✅ Đã xuất TXT {kind.upper()} · {count} {"kết quả đã lưu" if include_all else "phiên có dự đoán"}.'
        elif cmd in ('/exportbcr','/txtbcr','/exportbcrall','/exportall','/txtall','/exportallresults','/setbcrapi'):
            out='ℹ️ Baccarat đã được tắt ở V16. Chỉ còn Sunwin và MD5/SHA256.'
        elif cmd=='/resetlinks':
            with LOCK:CONFIG.clear();CONFIG.update(DEFAULT_CONFIG)
            save_config();out='✅ Đã khôi phục link/API mặc định.'
        else:
            key={'/setsunlink':'sunwin_game_url','/setsunapi':'sunwin_current_api','/setsunhistory':'sunwin_history_api','/setsunfallback':'sunwin_current_fallback'}.get(cmd)
            if key:
                if not arg:raise ValueError('Thiếu URL.')
                arg=safe_url(arg)
                with LOCK:CONFIG[key]=arg
                save_config();out=f'✅ Đã cập nhật <b>{key}</b>\n<code>{html.escape(arg)}</code>'
            else:out=admin_home()
    except Exception as e:out='❌ '+html.escape(str(e))
    page=({'/bias':'bias','/modelhealth':'bias','/diagnose':'system','/exportmenu':'export',
           '/loc':'home','/qr':'home','/quant':'quant','/modelcheck':'quant','/ips':'ips','/ip':'ips','/banip':'ips','/unbanip':'ips','/capip':'ips','/thuhoip':'ips','/live':'live','/liveaudit':'live','/dbstatus':'dbstatus','/auditdata':'auditdata','/dataaudit':'auditdata','/stats':'stats','/learn':'stats','/boards':'boards',
           '/panel':'overview','/overview':'overview','/help':'help','/huongdan':'help','/settings':'settings','/system':'system',
           '/warmup':'warmup','/auto30':'warmup','/md5stats':'md5stats','/v15':'v15','/auditv15':'v15','/yeucau':'access','/quyenweb':'access','/capquyen':'access','/thuhoi':'access', '/algo':'algo','/topalgo':'algo','/api':'api','/status':'api','/links':'links'}).get(cmd,'home')
    if cmd=='/ip' and arg:
        try:page='ipview:'+str(ipaddress.ip_address(arg.split()[0]))
        except ValueError:pass
    await send_admin(client,chat_id,out,True,page)


async def admin_edit_panel(client,msg,chat_id,content,keyboard):
    rendered=html.unescape(re.sub(r'<[^>]+>','',content))
    if msg.get('text')==rendered and msg.get('reply_markup')==keyboard:return
    payload={'chat_id':chat_id,'message_id':msg.get('message_id'),
             'text':content[:3900],'parse_mode':'HTML','disable_web_page_preview':True,
             'reply_markup':keyboard}
    if msg.get('message_id') and await tg(client,'editMessageText',payload) is not None:return
    if msg.get('text')==rendered:return  # unchanged text; avoid duplicate on Telegram 'not modified'
    await tg(client,'sendMessage',{'chat_id':chat_id,'text':content[:3900],
                                 'parse_mode':'HTML','disable_web_page_preview':True,
                                 'reply_markup':keyboard})


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
    if page=='ips' or page.startswith('ips:'):
        try:
            if page=='ips':mode,number='recent',0
            else:
                _,mode,number=page.split(':',2)
                number=int(number)
            content=ip_dashboard(mode,number)
            keyboard=admin_keyboard(f'ips:{mode}:{number}')
            await tg(client,'answerCallbackQuery',{'callback_query_id':q.get('id')})
            await admin_edit_panel(client,msg,chat_id,content,keyboard)
        except Exception as exc:
            await tg(client,'answerCallbackQuery',{'callback_query_id':q.get('id'),
                     'text':'Không tải được IP: '+str(exc)[:120],'show_alert':True})
        return
    if page.startswith('ipview:'):
        ip=page[len('ipview:'):]
        try:
            content=ip_detail(ip)
            await tg(client,'answerCallbackQuery',{'callback_query_id':q.get('id')})
            await admin_edit_panel(client,msg,chat_id,content,admin_keyboard(page))
        except Exception as exc:
            await tg(client,'answerCallbackQuery',{'callback_query_id':q.get('id'),
                     'text':str(exc)[:170],'show_alert':True})
        return
    if page.startswith('ipgrant:'):
        try:
            _,duration,ip=page.split(':',2)
            ip=str(ipaddress.ip_address(ip))
            seconds=parse_ip_duration(duration)
            content=(f'<b>◈ HTUNG / CẤP QUYỀN IP</b>\n━━━━━━━━━━━━━━━━━━━━\n'
                     f'🌐 <code>{html.escape(ip)}</code>\n'
                     f'⏳ Thời hạn: <b>{human_duration(seconds)}</b>\n\n'
                     '<i>Quyền áp dụng cho mọi thiết bị dùng chung IP này. Xác nhận cấp?</i>')
            await tg(client,'answerCallbackQuery',{'callback_query_id':q.get('id')})
            await admin_edit_panel(client,msg,chat_id,content,admin_keyboard(page))
        except Exception as exc:
            await tg(client,'answerCallbackQuery',{'callback_query_id':q.get('id'),'text':str(exc)[:170],'show_alert':True})
        return
    if page.startswith('ipgrantdo:'):
        try:
            _,duration,ip=page.split(':',2)
            ip,_=ip_grant_action(ip,'grant',duration,uid)
            await tg(client,'answerCallbackQuery',{'callback_query_id':q.get('id'),'text':'✅ Đã cấp quyền IP'})
            await admin_edit_panel(client,msg,chat_id,ip_detail(ip),admin_keyboard('ipview:'+ip))
        except Exception as exc:
            await tg(client,'answerCallbackQuery',{'callback_query_id':q.get('id'),'text':str(exc)[:170],'show_alert':True})
        return
    if page.startswith('iprevoke:'):
        try:
            ip=str(ipaddress.ip_address(page.split(':',1)[1]))
            ip_grant_action(ip,'revoke')
            await tg(client,'answerCallbackQuery',{'callback_query_id':q.get('id'),'text':'🔒 Đã thu hồi quyền IP'})
            await admin_edit_panel(client,msg,chat_id,ip_detail(ip),admin_keyboard('ipview:'+ip))
        except Exception as exc:
            await tg(client,'answerCallbackQuery',{'callback_query_id':q.get('id'),'text':str(exc)[:170],'show_alert':True})
        return
    if page.startswith('ipconfirm:'):
        try:
            _,action,ip=page.split(':',2)
            if action not in ('ban','unban'):raise ValueError('Thao tác không hợp lệ')
            ip=str(ipaddress.ip_address(ip))
            title='⛔ CHẶN IP' if action=='ban' else '✅ MỞ IP'
            content=(f'<b>◈ HTUNG / CONFIRM ACTION</b>\n'
                     f'━━━━━━━━━━━━━━━━━━━━\n<b>{title}</b>\n'
                     f'<code>{html.escape(ip)}</code>\n\n'
                     '<i>Thao tác có hiệu lực trên API Railway. Xác nhận để tiếp tục hoặc Hủy để quay lại.</i>')
            await tg(client,'answerCallbackQuery',{'callback_query_id':q.get('id')})
            await admin_edit_panel(client,msg,chat_id,content,admin_keyboard(page))
        except Exception as exc:
            await tg(client,'answerCallbackQuery',{'callback_query_id':q.get('id'),
                     'text':str(exc)[:170],'show_alert':True})
        return
    if page.startswith('ipdo:'):
        try:
            _,action,ip=page.split(':',2)
            if action not in ('ban','unban'):raise ValueError('Thao tác không hợp lệ')
            ip,_,_=ip_action(ip,action,'Admin đã xác nhận trong menu Telegram')
            await tg(client,'answerCallbackQuery',{'callback_query_id':q.get('id'),
                     'text':'⛔ Đã chặn IP' if action=='ban' else '✅ Đã mở IP'})
            await admin_edit_panel(client,msg,chat_id,ip_detail(ip),admin_keyboard('ipview:'+ip))
        except Exception as exc:
            await tg(client,'answerCallbackQuery',{'callback_query_id':q.get('id'),
                     'text':str(exc)[:170],'show_alert':True})
        return
    if page.startswith('banip:') or page.startswith('unbanip:'):
        # Old V16 buttons must not bypass the V17 confirmation screen.
        old_action,ip=page.split(':',1)
        try:
            ip=str(ipaddress.ip_address(ip))
            action='ban' if old_action=='banip' else 'unban'
            confirm='ipconfirm:'+action+':'+ip
            await tg(client,'answerCallbackQuery',{'callback_query_id':q.get('id')})
            await admin_edit_panel(client,msg,chat_id,
               ('<b>◈ HTUNG / CONFIRM ACTION</b>\n━━━━━━━━━━━━━━━━━━━━\n'
                +('⛔ <b>CHẶN IP</b>\n' if action=='ban' else '✅ <b>MỞ IP</b>\n')
                +f'<code>{html.escape(ip)}</code>\n\n<i>Chọn XÁC NHẬN hoặc HỦY.</i>'),
               admin_keyboard(confirm))
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
    if page in ('exportsun','exportsunall'):
        kind='sun'
        include_all=page=='exportsunall'
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
        'v15':v15_admin_text,
        'settings':admin_settings_text, 'help':admin_help_text,
        'iphelp':admin_ip_help_text, 'settingshelp':admin_settings_help_text,
        'quant':quant_report, 'ips':ip_overview, 'live':v10_live_report,
        'stats':stats_text, 'boards':boards_text, 'algo':algo_text, 'md5stats':md5_stats_text,
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
            {'command':'capip','description':'Cấp quyền IP có thời hạn'},{'command':'thuhoip','description':'Thu hồi quyền IP'},{'command':'help','description':'Hướng dẫn sử dụng'},{'command':'ips','description':'Xem IP truy cập'},{'command':'banip','description':'Chặn IP'},{'command':'unbanip','description':'Mở IP'},{'command':'live','description':'Đánh giá LIVE 20/50/100/200 phiên'},{'command':'stats','description':'Thống kê học cầu 24/7'},
            {'command':'boards','description':'Phiên/cầu đã học từng bàn'},{'command':'topalgo','description':'Top thuật toán đã kiểm tra'},
            {'command':'status','description':'Trạng thái API'},{'command':'links','description':'Xem link/API'},
            {'command':'exportsun','description':'Xuất TXT SUNWIN'},
            {'command':'exportsunall','description':'Xuất toàn bộ kết quả SUNWIN'},
            {'command':'md5stats','description':'Thống kê MD5 / SHA256'},
            {'command':'dbstatus','description':'Kiểm tra SQLite và phiên API'},
            {'command':'auditdata','description':'Kiểm toán mâu thuẫn dữ liệu'},
            {'command':'bias','description':'Chẩn đoán lệch dự đoán'},
            {'command':'diagnose','description':'Trung tâm kiểm tra'},
            {'command':'setsunlink','description':'Đổi link game SUNWIN'},{'command':'setsunapi','description':'Đổi API current SUNWIN'},
            {'command':'setsunhistory','description':'Đổi API history SUNWIN'},{'command':'setsunfallback','description':'Đổi API fallback SUNWIN'}]})
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
    load_config();ensure_db();init_deploy_warmup()
    with db() as con:
        v15.bootstrap(con,'sunwin:hu')
        con.commit()
    upgrade_existing_history();load_latest_from_db()
    threading.Thread(target=run_http,daemon=True).start()
    await asyncio.gather(cache_loop(),bot_loop())


if __name__=='__main__':
    try:asyncio.run(main_async())
    except KeyboardInterrupt:pass
