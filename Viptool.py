"""Statistical diagnostics adapted from inspected dvsr-tooltx/assets/js/algorithm.js.
No future labels, no claim of predicting random outcomes. Stdlib only.
"""
from math import log2, sqrt

def entropy(values):
    values=[x for x in values if x in ('TÀI','XỈU')]
    n=len(values)
    if not n:return None
    return -sum((c/n)*log2(c/n) for c in (values.count('TÀI'),values.count('XỈU')) if c)

def wilson(wins,n,z=1.96):
    if not n:return (0.,1.)
    p=wins/n; den=1+z*z/n
    mid=(p+z*z/(2*n))/den
    radius=z*sqrt(p*(1-p)/n+z*z/(4*n*n))/den
    return max(0.,mid-radius),min(1.,mid+radius)

def conditional_entropy(values):
    values=[x for x in values if x in ('TÀI','XỈU')]
    if len(values)<4:return None
    states={}
    for i in range(2,len(values)):
        k=tuple(values[i-2:i]);c=states.setdefault(k,[0,0]);c[0 if values[i]=='TÀI' else 1]+=1
    total=sum(sum(c) for c in states.values())
    return sum((-sum((x/s)*log2(x/s) for x in c if x))*s/total for c in states.values() if (s:=sum(c)))

def summarize(rows):
    a=[dict(r) for r in rows if r['actual'] in ('TÀI','XỈU') and r['prediction'] in ('TÀI','XỈU')]
    n=len(a);wins=sum(x['prediction']==x['actual'] for x in a)
    lo,hi=wilson(wins,n)
    return dict(n=n,wins=wins,rate=wins/n if n else None,lower=lo,upper=hi,
                entropy=entropy([x['actual'] for x in a]),
                conditional_entropy=conditional_entropy([x['actual'] for x in a]))
