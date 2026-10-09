#-----------------------------------------------------------------------------
# parse_ed_location_reports.py
#
# Copyright (c) 2026 Daniel M. Teal
#
# License: MIT License
#
# Parses the county ED_{REP,DEM}_Locations_Cumlative_Reports.pdf files (one
# cumulative report per Election Day site; each starts at "Page 1") into
# ed_locrep.json.  Run from the work directory containing locrep/ and out/.
#-----------------------------------------------------------------------------
import fitz,json,re,collections,sys
def parse(path):
    d=fitz.open(path); locs={}; n=0
    for pg in d:
        L=[x.strip() for x in pg.get_text().split('\n')]
        if 'Page 1' in L[:12]: n+=1
        loc='%03d|%s'%(n,L[0]); rec=locs.setdefault(loc,{'ballots':None,'tally':collections.defaultdict(dict)})
        i=0; contest=None
        while i<len(L):
            t=L[i]
            if t=='Ballots Cast': rec['ballots']=int(L[i+1]); i+=2; continue
            if re.search(r' - (Republican|Democratic) Party$',t) and L[i+1:i+2]==['Choice']:
                contest=re.sub(r' - (Republican|Democratic) Party$','',t); i+=1; continue
            if contest and t in('Undervotes:','Overvotes:'):
                rec['tally'][contest][t[:-1]]=int(L[i+1]); i+=3; continue
            if contest and t not in ('Choice','Party','Election Day Voting','Total','Cast Votes:') and i+4<len(L) \
               and re.fullmatch(r'\d+',L[i+1]) and L[i+2].endswith('%') and re.fullmatch(r'\d+',L[i+3]):
                rec['tally'][contest][t]=int(L[i+3]); i+=5; continue
            i+=1
    return locs
out={}
for p,f in (('rep','locrep/ED_REP_Locations_Cumlative_Reports.pdf'),('dem','locrep/ED_DEM_Locations_Cumlative_Reports.pdf')):
    out[p]=parse(f); print(p,len(out[p]),sum(v['ballots'] or 0 for v in out[p].values()))
json.dump(out,open('out/ed_locrep.json','w',encoding='utf-8'),ensure_ascii=False)
