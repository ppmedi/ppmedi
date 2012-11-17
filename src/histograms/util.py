import bsddb3
import re
import pdb
import json

from itertools import product
from collections import defaultdict
from sqlalchemy import *

re_col = re.compile('\d+$')

format_iter = lambda fmt, l: (fmt % el for el in l)


class HistogramMaker(object):
    def __init__(self, db, table):
        self.db = db
        self.table = table
        self.mapping = self.get_attr_mappings()
        self.schemadb = bsddb3.hashopen('./groupby.db')

    def tablenames(self):
        if 'tablenames' not in self.schemadb:
            return []
        return json.loads(self.schemadb['tablenames'])

    def info(self, tablename):
        if tablename not in self.schemadb:
            return None
        return json.loads(self.schemadb[tablename])

    def store(self, tablename, (data)):
        self.schemadb[tablename] = json.dumps(data)
        tablenames = self.tablenames()
        tablenames.append(tablename)
        self.schemadb['tablenames'] = json.dumps(tablenames)


    def base_cols(self):
        return self.mapping.keys()

    def get_attr_mappings(self):
        """
        create dictionary of base attribute name (dgnscd) to
        list of attribute names related (dgnscd1, dgnscd2,...)
        """
        mapping = defaultdict(list)
        with self.db.begin() as conn:
            cursor = db.execute("select * from %s limit 1" % self.table)
            cols = cursor.keys()

            for col in cols:
                postfix = re_col.findall(col)
                if not postfix:
                    mapping[col].append(col)
                else:
                    postfix = postfix[0]
                    col_base = col[:len(col) - len(postfix)]
                    mapping[col_base].append(col)
        return mapping

    def __call__(self, *args, **kwargs):
        return self.create_histogram(*args, **kwargs)

    def create_histogram(self, tablename, gbs, aggs=[]):
        """
        creates a histogram using aggs.
        always adds a column called 'count' for count(*)
        """
        # first create a retardedly large table with the 
        # cross product of all gbs
        combos = product(*[self.mapping[col] for col in gbs])
        agg_funcs, agg_cols = aggs and zip(*aggs) or ([], [])
        union_tablename = '%s_union' % tablename

        big_query = []
        for idx, combo in enumerate(combos):
            sel_cols = list(format_iter('%s::text as %s', zip(combo, gbs)))
            sel_cols.extend(agg_cols)
            sel_cols = ','.join(sel_cols)
            q = "(select %s from %s)" % (sel_cols, self.table)
            
            with self.db.begin() as conn:
                if idx == 0:
                    conn.execute("create table %s as %s" % (union_tablename, q))
                else:
                    conn.execute("insert into %s (%s)" % (union_tablename, q))


        #
        # now execute the groupby...
        #

        sel_aggs = ['%s(%s) as %s' % (ag, ac, ac) for  ag, ac in aggs]
        sel_cols = list(gbs)
        sel_cols.extend(sel_aggs)
        sel_cols = ','.join(sel_cols)
        q = "create table %s as select %s, count(*) as count from %s group by %s"
        q = q % (tablename, sel_cols, union_tablename, ','.join(gbs))

        with self.db.begin() as conn:
            conn.execute(q)

        # save this info somewhere!
        self.store(tablename, (gbs, aggs))
    
        
if __name__ == '__main__':
    db = create_engine('postgresql://localhost/penispros')
    hm = HistogramMaker(db, 'inp')
    print hm.base_cols()
    hm('diagnoses', ['provider', 'dgnscd'])
    hm('diagnoses_norm', ['provider'])
