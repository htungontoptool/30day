"""Server-only port of the 36-family / 288-signal MD5-SHA256 diagnostic engine.

These structural features are NOT evidence that a cryptographic hash predicts
random dice or baccarat results.  API outputs are diagnostic guesses, not odds.
"""
from __future__ import annotations
import hashlib
import math
import re
import statistics
import zlib
from collections import Counter

MODULE_NAMES = [
    'NIBBLE-DIST','BYTE-DIST','PARITY-FLOW','SYMMETRY','POSITIONAL','MOD-FOLD',
    'SHA256-MIX','SHA3-MIX','BLAKE2-MIX','ROTATION','CHUNK-VOTE','CHECKSUM',
    'SHA512-MIX','MD5-MIX','MIRROR-XOR','HAMMING-FLOW','WINDOW-FLOW','AVALANCHE-MIX',
    'FAMILY-BALANCE','HALF-CROSS','QUARTER-FLOW','HISTOGRAM-SKEW','LAG-SPECTRAL',
    'SEGMENT-STABILITY','ENTROPY-BANDS','DIGEST-CONSENSUS','ROBUST-STACK',
    'HEX-RUN-PROFILE','PRIME-POS-FLOW','BITPLANE-SPECTRUM','CROSS-DIGEST-CORR',
    'MULTISCALE-BLOCK','PERMUTATION-VOTE','RESIDUE-ENSEMBLE','PERTURB-STABILITY','META-CONSENSUS'
]
HASH_RE = re.compile(r'^(?:[0-9a-f]{32}|[0-9a-f]{64})$')

def clamp(x, lo=-1.0, hi=1.0):
    return max(lo, min(hi, float(x)))

def centered(x, mid, scale):
    return clamp((x-mid)/scale) if scale else 0.0

def avg(a):
    return sum(a)/len(a) if a else 0.0

def trim_mean(a):
    if not a:
        return 0.0
    s = sorted(a)
    if len(s)>=6:
        s=s[1:-1]
    return avg(s)

def chunks(xs, n=4):
    # JS Math.round for non-negative positions rounds half upwards.
    L=len(xs)
    return [xs[math.floor(i*L/n+.5):math.floor((i+1)*L/n+.5)] for i in range(n)]

def entropy(a):
    if not a: return 0.0
    return -sum((v/len(a))*math.log2(v/len(a)) for v in Counter(a).values())

def entropy01(a):
    if not a:return 0.0
    return entropy(a)/max(1,math.log2(len(set(a))))

def corr(a, lag):
    if len(a)<=lag+1:return 0.0
    mid=avg(a)
    den=sum((x-mid)**2 for x in a) or 1
    return clamp(sum((a[i]-mid)*(a[i-lag]-mid) for i in range(lag,len(a)))/den)

def transition(a):
    if len(a)<2:return 0.0
    return centered(sum(a[i]!=a[i-1] for i in range(1,len(a)))/(len(a)-1),.5,.5)

def rotate(a,n):
    return a[n%len(a):]+a[:n%len(a)] if a else a

def digest_signals(data):
    vals=list(data)
    bits=[(b>>k)&1 for b in vals for k in range(7,-1,-1)]
    out=[centered(avg(c),127.5,78) for c in chunks(vals,4)]
    out += [centered(avg(bits),.5,.22),corr(vals,1),corr(vals,3),transition([b>=128 for b in vals])]
    return [clamp(v) for v in out[:8]]

def group(a):
    return [clamp(v) for v in a[:8]]

def _hamming(a,b):
    return (a^b).bit_count()

def _is_prime(i):
    return i>=2 and all(i%d for d in range(2,math.isqrt(i)+1))

def _adler32(s):
    return zlib.adler32(s.encode()) & 0xffffffff

def _crc32(s):
    return zlib.crc32(s.encode()) & 0xffffffff

def _fallback_digest(text,length=32):
    # Exactly the fallback xorshift used in the older JS for b2b stubs.
    mask=0xffffffff
    h=2166136261
    for c in text:
        h^=ord(c)
        h=(h*16777619)&mask
    out=[]
    for _ in range(length):
        h^=(h<<13)&mask; h&=mask
        h^=h>>17; h&=mask
        h^=(h<<5)&mask; h&=mask
        out.append(h&255)
    return out

def _sha(algo, text):
    return list(hashlib.new(algo,text.encode('utf-8')).digest())

def _blake(text):
    return list(hashlib.blake2s(text.encode('utf-8')).digest())

def base18(z):
    nib=[int(x,16) for x in z]
    bs=list(bytes.fromhex(z))
    bits=[(b>>k)&1 for b in bs for k in range(7,-1,-1)]
    N=len(nib);B=len(bs);groups=[]
    g=[centered(avg(p),7.5,4.6) for p in chunks(nib,4)]
    g += [centered(sum(x>=8 for x in p)/max(1,len(p)),.5,.42) for p in chunks(nib,4)]
    groups.append(g)
    g=[centered(avg(p),127.5,78) for p in chunks(bs,4)]
    g += [centered(sum(x%2==0 for x in p)/max(1,len(p)),.5,.42) for p in chunks(bs,4)]
    groups.append(g)
    groups.append([transition([x>=8 for x in nib]),transition([x&1 for x in nib]),transition(bits),centered(avg(bits),.5,.22),centered(sum(x%2==0 for x in nib)/N,.5,.42),centered(sum(x in (0,15) for x in nib)/N,.125,.16),corr(nib,1),corr(nib,2)])
    rev=nib[::-1];half=N//2
    groups.append([centered(avg(nib[:half])-avg(nib[half:]),0,4.8),centered(avg([abs(x-rev[i]) for i,x in enumerate(nib)]),5.3,3),centered(len(set(nib)),12 if N==32 else 15,4),centered(entropy(nib),3.72 if N==32 else 3.86,.34),corr(nib,3),corr(nib,5),centered(avg(nib[::2]),7.5,4.6),centered(avg(nib[1::2]),7.5,4.6)])
    iA=[i for i in range(N) if i%3==0];iB=[i for i in range(N) if i%3==1];iC=[i for i in range(N) if i%3==2]
    primes=[i for i in range(N) if _is_prime(i)]
    fib=[i for i in [0,1,2,3,5,8,13,21,34,55] if i<N]
    sel=lambda inds:avg([nib[i] for i in inds])
    wsum=sum((i+1)*v for i,v in enumerate(nib))%257
    groups.append([centered(sel(inds),7.5,4.6) for inds in [iA,iB,iC,primes,fib,[i for i in range(N) if i%4==0],[i for i in range(N) if i%4==1]]] + [centered(wsum,128,128)])
    xorb=0
    for b in bs:xorb^=b
    pairxor=[nib[i]^nib[(i+1)%N] for i in range(0,N,2)]
    first=int(z[:min(16,N)],16);last=int(z[-min(16,N):],16)
    groups.append([centered(sum(nib)%17,8,8),centered(xorb%17,8,8),centered(float(first)%97,48,48),centered(float(last)%97,48,48),centered(avg(pairxor),7.5,4.6),centered(avg([v^nib[(i+7)%N] for i,v in enumerate(nib)]),7.5,4.6),centered(avg([(v+nib[(i+11)%N])%16 for i,v in enumerate(nib)]),7.5,4.6),centered(avg([(v-nib[(i+5)%N]+16)%16 for i,v in enumerate(nib)]),7.5,4.6)])
    sha=_sha('sha256',z);s3=_sha('sha3_256',z);bl=_blake(z)
    groups.extend([digest_signals(sha),digest_signals(s3),digest_signals(bl)])
    r3,r7,r13=[rotate(nib,k) for k in (3,7,13)]
    groups.append([centered(avg([abs(x-r[i]) for i,x in enumerate(nib)]),5.3,3) for r in (r3,r7,r13)] + [centered(avg([x^r[i] for i,x in enumerate(nib)]),7.5,4.6) for r in (r3,r7,r13)] + [corr(nib,7),corr(nib,11 if N>32 else 9)])
    groups.append([centered(avg(p),7.5,4.6) for p in chunks(nib,8)])
    crc=_crc32(z);ad=_adler32(z);sha512=_sha('sha512',z);d1=_fallback_digest('b2b:'+z,8)
    u16=lambda a,i:((a[i] if i<len(a) else 0)<<8)|(a[i+1] if i+1<len(a) else 0)
    groups.append([centered(crc%257,128,128),centered(ad%257,128,128),centered(u16(d1,0)%257,128,128),centered(u16(d1,2)%257,128,128),centered(u16(sha512,0)%257,128,128),centered(u16(sha512,2)%257,128,128),centered(avg(bs[::2]),127.5,78),centered(avg(bs[1::2]),127.5,78)])
    groups.append(digest_signals(sha512[:32]))
    md=_sha('md5',z);md2=list(hashlib.md5(bytes(md).hex().encode()).digest())
    groups.append(digest_signals(md+md2))
    revb=bs[::-1]
    groups.append([centered(avg([a^revb[i] for i,a in enumerate(bs)]),127.5,78),centered(avg([abs(a-revb[i]) for i,a in enumerate(bs)]),85,55),centered(avg([_hamming(a,revb[i]) for i,a in enumerate(bs)]),4,3),corr(bs,2),corr(bs,5),transition([a>revb[i] for i,a in enumerate(bs)]),centered(avg([(a+revb[i])&255 for i,a in enumerate(bs)]),127.5,78),centered(avg([(a-revb[i]+256)&255 for i,a in enumerate(bs)]),127.5,78)])
    g=[]
    for lag in (1,2,3,5):
        g.append(centered(avg([_hamming(bs[i],bs[i-lag]) for i in range(lag,B)]),4,3))
    g.extend([corr(bits,1),corr(bits,7),transition(bits),centered(avg(bits),.5,.22)])
    groups.append(g)
    g=[]
    for w in (3,4,5,7,8,11,13,16):
        av=[avg(nib[i:i+w]) for i in range(N-w+1)] if N>=w else []
        g.append(centered(av[-1]-avg(av),0,3.8) if av else 0.0)
    groups.append(g)
    g=[]
    for i in range(min(8,len(z))):
        arr=list(z);arr[i]=hex(int(arr[i],16)^1)[2:]
        dd=_blake(''.join(arr));ham=sum(_hamming(dd[j],bl[j]) for j in range(min(len(dd),len(bl))))
        g.append(centered(ham/(min(len(dd),len(bl))*8),.5,.28))
    groups.append((g+[0]*8)[:8])
    groups=[group(g) for g in groups[:18]]
    assert len(groups)==18 and all(len(g)==8 for g in groups)
    return groups

def level27(z):
    groups=base18(z)
    nib=[int(x,16) for x in z];bs=list(bytes.fromhex(z));N=len(nib);B=len(bs)
    s=[trim_mean(g) for g in groups]
    fams=[trim_mean([s[i] for i in ids]) for ids in [[0,1,4],[2,3,14],[5,10,11],[6,7,8],[12,13,17],[9,15,16]]]
    groups.append(group(fams+[trim_mean(fams),centered(sum(x>0 for x in fams),3,2.1)]))
    hn=N//2;hb=B//2;a=nib[:hn];b=nib[hn:];ab=bs[:hb];bb=bs[hb:]
    groups.append(group([centered(avg(a)-avg(b),0,4.8),centered(avg(ab)-avg(bb),0,82),centered(entropy01(a)-entropy01(b),0,.35),corr(nib,max(1,hn-1)),centered(avg([abs(v-b[i%len(b)]) for i,v in enumerate(a)]),5.2,3.2),transition([v>nib[(i+hn)%N] for i,v in enumerate(nib)]),centered(avg([v^b[i%len(b)] for i,v in enumerate(a)]),7.5,4.6),trim_mean([s[0],s[3],s[14],s[16]])]))
    q=max(1,N//4);qs=[nib[i*q:N if i==3 else (i+1)*q] for i in range(4)]
    qm=[avg(x) for x in qs];qe=[entropy01(x) for x in qs]
    groups.append(group([centered(qm[3]-qm[0],0,4.5),centered(qm[2]-qm[1],0,4.5),centered((qm[0]+qm[3])-(qm[1]+qm[2]),0,7),centered(qe[3]-qe[0],0,.35),centered(qe[2]-qe[1],0,.35),transition([v>qm[(i+1)%4] for i,v in enumerate(qm)]),centered(max(qm)-min(qm),3.5,3),trim_mean([s[4],s[9],s[10],s[16]])]))
    hist=[nib.count(i) for i in range(16)]
    lo=sum(hist[:8]);hi=sum(hist[8:]);even=sum(hist[::2]);odd=N-even
    groups.append(group([centered((hi-lo)/N,0,.45),centered((even-odd)/N,0,.45),centered((hist[0]+hist[15])/N,.125,.12),centered(max(hist)/N,1/16,.15),centered(entropy01(nib),.82,.22),centered(avg(hist[:4])-avg(hist[12:]),0,N/10),trim_mean([s[0],s[1],s[5]]),trim_mean([s[11],s[14]])]))
    groups.append(group([corr(nib,l) for l in [1,2,3,5,7,11 if N>32 else 9]] + [trim_mean([s[9],s[15],s[16]]),centered(sum(corr(nib,l)>0 for l in [1,2,3,5,7]),2.5,2.2)]))
    segs=chunks(nib,max(4,N//8));sm=[avg(x) for x in segs];se=[entropy01(x) for x in segs]
    groups.append(group([centered(max(sm)-min(sm),3.2,3),centered(max(se)-min(se),.25,.28),centered(sm[-1]-sm[0],0,4.5),centered(se[-1]-se[0],0,.35),transition([v>sm[(i+1)%len(sm)] for i,v in enumerate(sm)]),corr(sm,1),trim_mean([s[10],s[16]]),trim_mean([s[2],s[4],s[15]])]))
    low=[v for v in nib if v<4];mid=[v for v in nib if 4<=v<12];high=[v for v in nib if v>=12]
    groups.append(group([centered(entropy01(nib),.82,.22),centered(entropy01(bs),.9,.2),centered(len(low)/N,.25,.25),centered(len(high)/N,.25,.25),centered(len(mid)/N,.5,.3),centered((len(high)-len(low))/N,0,.4),trim_mean([s[0],s[1],s[11]]),trim_mean([s[6],s[7],s[8],s[12],s[13]])]))
    dig=s[6:14];dp=sum(x>0 for x in dig);dn=sum(x<0 for x in dig)
    groups.append(group([trim_mean(dig),avg(dig),centered(dp-dn,0,5),trim_mean([s[6],s[12]]),trim_mean([s[7],s[13]]),trim_mean([s[8],s[17]]),trim_mean([s[6],s[7],s[8]]),trim_mean([s[12],s[13],s[17]])]))
    pre=[trim_mean(g) for g in groups];so=sorted(pre);med=so[len(so)//2]
    groups.append(group([trim_mean(pre),avg(pre),med,centered(sum(x>.02 for x in pre)-sum(x<-.02 for x in pre),0,7),trim_mean(pre[:9]),trim_mean(pre[9:18]),trim_mean(pre[18:]),trim_mean(fams)]))
    assert len(groups)==27 and all(len(g)==8 for g in groups)
    return groups

def scores288(z):
    z=z.lower().strip()
    if not HASH_RE.fullmatch(z):
        raise ValueError('Hash phải là MD5 32 HEX hoặc SHA256 64 HEX')
    groups=level27(z)
    nib=[int(x,16) for x in z];N=len(nib);bs=list(bytes.fromhex(z))
    flags=[v>=8 for v in nib];runs=[];i=0
    while i<N:
        j=i+1
        while j<N and flags[j]==flags[i]:j+=1
        runs.append(j-i);i=j
    groups.append(group([centered(avg(runs),2,2.4),centered(max(runs),4,4),centered(len(runs),N/2,N/2),transition(flags),centered(sum(x>=3 for x in runs),max(1,len(runs))/4,max(1,len(runs))/2),centered(sum(x%2 for x in runs)/max(1,len(runs)),.5,.5),corr(runs,1),corr(runs,2)]))
    prime=[i for i in range(2,N) if _is_prime(i)];ps=set(prime);pv=[nib[i] for i in prime];nv=[v for i,v in enumerate(nib) if i not in ps]
    groups.append(group([centered(avg(pv)-avg(nv),0,4.5),centered(entropy01(pv)-entropy01(nv),0,.35),centered(sum(v>=8 for v in pv)/len(pv),.5,.5),centered(sum(v%2 for v in pv)/len(pv),.5,.5),corr(pv,1),corr(pv,2),centered(sum((i+1)*nib[i] for i in prime)%127,63,63),centered(sum(pv)%61,30,30)]))
    planes=[[v>>bit&1 for v in nib] for bit in range(4)]
    groups.append(group([centered(avg(p),.5,.5) for p in planes] + [transition(planes[0]),transition(planes[1]),trim_mean([corr(p,1) for p in planes]),trim_mean([corr(p,2) for p in planes])]))
    dsha=_sha('sha256',z);ds3=_sha('sha3_256',z);dm=_sha('md5',z)
    def pearson(a,b):
        n=min(len(a),len(b));a=a[:n];b=b[:n];ma=avg(a);mb=avg(b)
        top=sum((a[i]-ma)*(b[i]-mb) for i in range(n));va=sum((v-ma)**2 for v in a);vb=sum((v-mb)**2 for v in b)
        return clamp(top/math.sqrt(va*vb)) if va and vb else 0.0
    groups.append(group([pearson(dsha,ds3),pearson(dsha,dm),pearson(ds3,dm),pearson(dsha,bs),pearson(ds3,bs),pearson(dm,bs),centered(avg(dsha)-avg(ds3),0,60),centered(avg(dsha)-avg(dm),0,60)]))
    multi=[]
    for block in [2,4,8,16]:
        cs=[avg(nib[i:i+block]) for i in range(0,N,block)];multi.extend([centered(max(cs)-min(cs),3.5,3.5),corr(cs,1)])
    groups.append(group(multi))
    ix=list(range(N));orders=[ix,ix[::-1],[*ix[::2],*ix[1::2]],[*ix[1::2],*ix[::2]],sorted(ix,key=lambda i:(i%4,i)),sorted(ix,key=lambda i:(i%3,i)),sorted(ix,key=lambda i:(i*5)%N),sorted(ix,key=lambda i:(i*7)%N)]
    votes=[]
    for order in orders:
        arr=[nib[i] for i in order];k=max(1,N//3)
        votes.append(trim_mean([centered(avg(arr[:k])-avg(arr[-k:]),0,4.5),corr(arr,1),corr(arr,2),centered(sum((j+1)*v for j,v in enumerate(arr))%(16*N),8*N,8*N)]))
    groups.append(group(votes))
    val=int(z,16)
    groups.append(group([centered(val%m,(m-1)/2,(m-1)/2) for m in [17,29,31,43,61,73,97,127]]))
    pert=[nib[1:]+nib[:1],nib[3:]+nib[:3],nib[::-1],[v^15 for v in nib],[v^nib[N-i-1] for i,v in enumerate(nib)],[*nib[::2],*nib[1::2]],[*nib[1::2],*nib[::2]], [((v<<1)&15)|(v>>3) for v in nib]]
    groups.append(group([trim_mean([centered(avg(seq),7.5,5),corr(seq,1),corr(seq,2),centered(sum(v>=8 for v in seq)/N,.5,.5),centered(entropy01(seq),.82,.24)]) for seq in pert]))
    pre=[trim_mean(g) for g in groups];ss=sorted(pre);median=ss[len(ss)//2]
    fam=[trim_mean(pre[i:i+3]) for i in range(0,33,3)]
    groups.append(group([trim_mean(pre),avg(pre),median,trim_mean(fam),avg(fam),centered(sum(v>.015 for v in pre)-sum(v<-.015 for v in pre),0,10),trim_mean(pre[33:35]),trim_mean(pre[-9:])]))
    assert len(groups)==36 and all(len(g)==8 for g in groups), (len(groups),[len(g) for g in groups])
    return [trim_mean(g) for g in groups]


def weighted_result(z, history):
    """history: sequence of {hash,actual,scores} for the same approved browser."""
    z=z.lower().strip()
    if not HASH_RE.fullmatch(z):raise ValueError('Hash phải là MD5 32 HEX hoặc SHA256 64 HEX')
    kind='MD5' if len(z)==32 else 'SHA256'
    scores=scores288(z)
    labs=[r for r in history if r['hash']!=z and r['kind']==kind]
    rows=[];weighted=[]
    for idx,(name,sc) in enumerate(zip(MODULE_NAMES,scores)):
        tp=tn=fp=fn=0
        for lab in labs:
            old=float(lab['scores'][idx]);guess='TÀI' if old>0 else 'XỈU'
            actual=lab['actual']
            if guess=='TÀI' and actual=='TÀI':tp+=1
            elif guess=='TÀI':fp+=1
            elif actual=='XỈU':tn+=1
            else:fn+=1
        total=tp+tn+fp+fn
        tpr=(tp+2)/(tp+fn+4);tnr=(tn+2)/(tn+fp+4);bacc=(tpr+tnr)/2
        m=min(1,total/80);w=1+clamp((bacc-.5)*1.4*m,-.22,.22)
        ws=sc*w;weighted.append(ws)
        rows.append(dict(name=name,score=sc,weighted=ws,pred='TÀI' if sc>0 else 'XỈU',w=w,total=total,bacc=bacc))
    families=[trim_mean(weighted[i:i+3]) for i in range(0,36,3)]
    family_raw=trim_mean(families);global_raw=trim_mean(weighted)
    raw=.60*family_raw+.25*global_raw+.15*trim_mean(sorted(weighted)[12:24])-.024
    sig=''.join('T' if x>=0 else 'X' for x in scores[:8])
    sig_labs=[r for r in labs if ''.join('T' if x>=0 else 'X' for x in r['scores'][:8])==sig]
    n_sig=len(sig_labs);sig_p=(sum(r['actual']=='TÀI' for r in sig_labs)+3)/(n_sig+6)
    bias_p=(sum(r['actual']=='TÀI' for r in labs)+8)/(len(labs)+16) if labs else .5
    if n_sig>=8:raw+=clamp((sig_p-.5)*.035*min(1,n_sig/48),-.012,.012)
    if len(labs)>=32:raw-=clamp((bias_p-.5)*.016*min(1,len(labs)/160),-.006,.006)
    pos=sum(v>.008 for v in families);neg=sum(v<-.008 for v in families)
    active=pos+neg;agreement=max(pos,neg)/active if active else .5
    module_pos=sum(v>.01 for v in weighted);module_neg=sum(v<-.01 for v in weighted)
    module_agree=max(module_pos,module_neg)/(module_pos+module_neg) if module_pos+module_neg else .5
    stability=.62*agreement+.38*module_agree
    if abs(raw)<.0032:
        raw=.0034 if hashlib.sha256(('ULTRA288:'+kind+':'+z).encode()).digest()[0]&1 else -.0034
    strength=50+min(8.8,abs(raw)*16.5+max(0,stability-.5)*7.2+min(1,len(labs)/160)*.55)
    strength=clamp(strength,50.1,58.8)
    level='MẠNH' if strength>=57 and stability>=.68 else 'VỪA' if strength>=54.2 and stability>=.59 else 'NHẸ'
    rows.sort(key=lambda r:abs(r['weighted']),reverse=True)
    return dict(ok=True,kind=kind,prediction='TÀI' if raw>0 else 'XỈU',strength=strength,
                agreement=stability*100,pos=sum(v>0 for v in families),neg=sum(v<0 for v in families),
                level=level,learned=len(labs),signature=sig,signatureN=n_sig,rows=rows[:10],
                familyScores=families,engine='ULTRA-288-PY',note='Điểm cấu trúc, không phải xác suất thắng')