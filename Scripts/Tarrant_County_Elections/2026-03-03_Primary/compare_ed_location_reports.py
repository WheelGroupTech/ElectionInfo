#-----------------------------------------------------------------------------
# compare_ed_location_reports.py
#
# Copyright (c) 2026 Daniel M. Teal
#
# License: MIT License
#
# Compares ed_locrep.json (parse_ed_location_reports.py) with the CVRs grouped
# by polling place, every contest/choice/undervote/overvote line.  Sites are
# matched by name, falling back to identical ballot counts (which exposes
# mislabelled reports).  Run from the work directory (out/).
#-----------------------------------------------------------------------------
import json,collections,re
def nm(s):
    s=re.sub(r'^(REP|DEM)\s+','',s or ''); return re.sub(r'[^a-z0-9]','',s.lower())
ALIAS={'agapeunitedchristian':'edlocation201','alphainternationalseventh':'edlocation202','houseofprayerandpraise':'edlocation203','jamesstarrettelementary':'edlocation204'}
LR=json.load(open('ed_locrep.json',encoding='utf-8'))
res={}
for P in ('rep','dem'):
    place={}
    for l in open(f'pdf_{P}_ed.jsonl',encoding='utf-8'):
        r=json.loads(l); place[r['Cvr Id']]=r['Polling Place']
    cv=collections.defaultdict(lambda: collections.defaultdict(collections.Counter)); nb=collections.Counter()
    for l in open(f'zip_{P}_ed.jsonl',encoding='utf-8'):
        r=json.loads(l); k=nm(place[r['guid']])
        if r['sheet']=='1': nb[k]+=1
        for cn,opts,u,o in r['contests']:
            if o: cv[k][cn]['Overvotes']+=1; continue
            for on,v,w in opts: cv[k][cn][on]+=int(v or 1)
            if u: cv[k][cn]['Undervotes']+=int(u)
    lr={nm(k.split('|',1)[1])+'#'+k.split('|')[0]:v for k,v in LR[P].items()}
    def find(k):
        k=k.split('#')[0]; k=ALIAS.get(k,k)
        if k in nb: return k
        import os
        best=max(nb,key=lambda x:(len(os.path.commonprefix([x,k])),-abs(len(x)-len(k))))
        return best if len(os.path.commonprefix([best,k]))>=12 else None
    used=set(); bad=[]; agree=0; lines=0
    for k,v in lr.items():
        same=[x for x in nb if nb[x]==v['ballots'] and x not in used]
        c=find(k)
        if c is None or nb[c]!=v['ballots']:
            import os
            if same:
                c2=max(same,key=lambda x:len(os.path.commonprefix([x,k.split('#')[0]])))
                bad.append(('RELABEL',k,v['ballots'],'-> CVR place',c2)); c=c2
        if c is None: bad.append(('NO CVR MATCH',k,v['ballots'])); continue
        used.add(c)
        if v['ballots']!=nb[c]: bad.append(('BALLOTS',k,v['ballots'],nb[c]))
        for cn,ch in v['tally'].items():
            for name,n in ch.items():
                lines+=1
                lk={re.sub('[^a-z]','',a.lower()).replace('sorrells','sorrels'):b for a,b in cv[c][cn].items()}
                g=lk.get(re.sub('[^a-z]','',name.lower()).replace('sorrells','sorrels'),0)
                if g!=n: bad.append(('VOTES',k,cn,name,n,g))
                else: agree+=1
    print(P,'locations',len(lr),'lines',lines,'agree',agree,'issues',len(bad))
    for b in bad[:40]: print('  ',b)
    print('  CVR places without location report:',{k:nb[k] for k in nb if k not in used})
