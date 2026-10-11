"""Conservative audit-only guard for binary next-round labels.

This is NOT a win-rate optimizer. A signal can pass these tests by chance;
LIVE-only tracking and explicit abstention prevent false assurance.
"""
from math import sqrt


def wilson_lower(wins, total, z=1.645):
    if total <= 0:
        return 0.0
    p = wins / total
    zz = z*z
    return max(0.0, (p + zz/(2*total) - z*sqrt((p*(1-p)+zz/(4*total))/total)) / (1+zz/total))


def gate_live(con, board, source_len, has_champion):
    """Assess only LIVE predictions issued before settlement on THIS board."""
    rows = con.execute("""SELECT ok FROM prediction_log
        WHERE board=? AND source='live'
        ORDER BY created_at DESC LIMIT 120""", (board,)).fetchall()
    vals = [int(r['ok']) for r in rows]
    n = len(vals)
    w = sum(vals)
    recent = vals[:15]
    r15 = sum(recent)
    lb = wilson_lower(w, n)
    stats = {'live_n':n, 'live_correct':w,
             'live_rate':round(w/n*100,1) if n else None,
             'recent15_n':len(recent), 'recent15_correct':r15,
             'recent15_rate':round(r15/len(recent)*100,1) if recent else None,
             'live_lower_bound':round(lb*100,1)}
    # Priority: protect against an observable recent error streak.
    if len(recent)==15 and r15<=6:
        code,reason='RECENT_LOSSES',f'15 dự đoán LIVE gần nhất chỉ đúng {r15}/15; nên tạm ngừng'
    elif source_len < 60:
        code,reason='INSUFFICIENT_HISTORY','Chưa có 60 kết quả liên tục để kiểm tra tín hiệu'
    elif n < 60:
        code,reason='INSUFFICIENT_LIVE',f'Mới có {n}/60 dự đoán LIVE được xác nhận'
    elif not has_champion:
        code,reason='NO_CHAMPION','Chưa có mô hình vượt được mức đối chứng lịch sử'
    elif lb<=.5:
        code,reason='NOT_VERIFIED','Kết quả LIVE chưa chứng minh vượt mốc ngẫu nhiên'
    else:
        code,reason='EVIDENCE_PRESENT','Có tín hiệu thống kê từ LIVE; vẫn có rủi ro cao'
    return {'signal_status':'watch' if code=='EVIDENCE_PRESENT' else 'skip',
            'signal_reason':reason,'signal_code':code,'live_stats':stats,
            'signal_is_win_probability':False}
