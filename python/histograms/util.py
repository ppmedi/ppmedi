import bsddb3
import re
import pdb
import json

from itertools import product
from collections import defaultdict
from sqlalchemy import *

re_col = re.compile('\d+$')
format_iter = lambda fmt, l: (fmt % el for el in l)
fix_star_for_str = lambda aggs: [(ag, ac, '_count_' if ac == '*' else ac) for
        ag, ac in aggs]

class get_cache():
    def __init__(self, fname):
        self.db = None 
        self.fname = fname

    def __enter__(self):
        self.db = bsddb3.hashopen(self.fname)
        return self.db

    def __exit__(self, type, value, traceback):
        try:
            self.db.close()
        except:
            pass

class HistogramMaker(object):
    def __init__(self, db, table):
        self.db = db
        self.table = table
        self.cachename = './gbcache.db'

    def __call__(self, tablename, join_cols, agg_group_cols, aggs, colrange=None):
        """
        @param tablename resulting tablename that histogram should be stored in
        @param join_cols [provider]
        @param agg_group_cols [diagnosis] 
               Note diagnosis has multiple columns in the database
               e.g., diag1, diag2,... diag9
        @param aggs [(agg_func, agg_colname)]
        """
        # NOTE: aggs is now [(agg_func, agg_arg, agg_name)*]
        aggs = fix_star_for_str(aggs)

        # first create union of the columns
        union_maker = UnionMaker(self.db, self.table)
        union_tablename = union_maker(tablename, join_cols+agg_group_cols, aggs=aggs, colrange=colrange) 

        # execute group by
        self.execute_groupby(tablename, union_tablename, join_cols+agg_group_cols, aggs=aggs)

        # normalize and return
        norm_subquery = self.construct_norm_query(tablename, join_cols, aggs)

        gbs = join_cols + agg_group_cols
        gb_exprs = format_iter('t.%s as %s', zip(gbs, gbs))
        gb_exprs = ','.join(gb_exprs)
        agg_exprs = [(ac, ac, ac) for ag, _, ac in aggs]
        agg_exprs = format_iter("t.%s::float/norm_t.%s as %s", agg_exprs)
        agg_exprs = ','.join(agg_exprs)
        select_clause = "SELECT %s, %s" % (gb_exprs, agg_exprs)
        from_clause = "FROM %s as t, (%s) as norm_t" % (tablename, norm_subquery)
        where_clause = "WHERE %s" % ' and '.join(format_iter("t.%s = norm_t.%s", zip(join_cols, join_cols)))

        query = "%s\n%s\n%s;" % (select_clause, 
                                 from_clause,
                                 where_clause)
        print query

        with self.db.begin() as conn:
            dists = defaultdict(list)
            for row in conn.execute(query):
                group = tuple(map(str, (row[col] for col in agg_group_cols)))
                #vals = dict([(ac, row[ac]) for ag, ac in aggs])
                # XXX: Hack to return only one statistic
                ag, ac, an = aggs[0]
                dists[group].append(row[an])
            return dists
        return None


    def construct_norm_query(self, tablename, join_cols, aggs):
        gbs = ','.join(join_cols)
        agg_exprs = []
        for ag, ac, an in aggs:
            if ag.lower() == 'count':
                ag = 'SUM'
            agg_exprs.append('%s(%s) as %s' % (ag, an, an))
        agg_exprs = ','.join(agg_exprs)
        norm_query = "SELECT %s, %s FROM %s GROUP BY %s" % (gbs, agg_exprs, tablename, gbs)
        return norm_query


    def execute_groupby(self, tablename, union_tablename, gbs, aggs=[]):
        key = str(tuple([tablename, union_tablename] + list(gbs) + list(aggs)))
        with get_cache(self.cachename) as cache:
            if key in cache:
                return

        sel_aggs = format_iter('%s(%s) as %s', aggs)
        sel_cols = list(gbs) + list(sel_aggs)
        sel_cols = ','.join(sel_cols)
        q = "drop table if exists %s; create table %s as select %s from %s group by %s"
        q = q % (tablename, tablename, sel_cols, union_tablename, ','.join(gbs))
        print q

        with self.db.begin() as conn:
            conn.execute(q)

        with get_cache(self.cachename) as cache:
            cache[key] = 'True'

 

class UnionMaker(object):
    def __init__(self, db, table):
        self.db = db
        self.table = table
        self.mapper = ColMapper(db, table)
        self.cachename = './groupby.db'


    def __call__(self, tablename, gbs, aggs=[], colrange=None):
        key = str(tuple( [tablename] + list(gbs) + list(aggs) + (list(colrange) if colrange else [])))
        with get_cache(self.cachename) as cache:
            if key in cache:
                return cache[key]

        union_tablename = self.disambiguate_table(tablename, gbs, aggs=[], colrange=None)

        with get_cache(self.cachename) as cache:
            cache[key] = union_tablename
        return union_tablename


    def disambiguate_table(self, tablename, gbs, aggs=[], colrange=None):
        """
        replaces the shortname column names in @gbs with
        the actual column names (e.g., gb = diag, actual = diag1, diag2,...)

        Constructs a union
        """
        # first create a retardedly large table with the 
        # cross product of all gbs
        combos = product(*[self.mapper(col, colrange) for col in gbs])
        print aggs
        aggs = filter(lambda triple: triple[1] != '*', aggs)
        agg_funcs, _, agg_cols = aggs and zip(*aggs) or ([], [], [])
        union_tablename = '%s_union' % tablename

        # XXX: not sure we should be taking the cross product of the columns...
        big_query = []
        first = True
        for idx, combo in enumerate(combos):
            sel_cols = list(format_iter('%s::text as %s', zip(combo, gbs)))
            sel_cols.extend(agg_cols)
            sel_cols = ','.join(sel_cols)
            q = "(select %s from %s)" % (sel_cols, self.table)
            
            with self.db.begin() as conn:
                if first:
                    print q
                    conn.execute("drop table if exists %s; create table %s as %s" % (union_tablename, union_tablename, q))
                    first = False
                else:
                    conn.execute("insert into %s (%s)" % (union_tablename, q))
        
        return union_tablename


   

class ColMapper(object):
    def __init__(self, db, table):
        self.db = db
        self.table = table
        self.mapping = self.get_attr_mappings()

    def __call__(self, col, colrange=None):
        cols = self.mapping.get(col, [col])
        if colrange and len(cols) > 1:
            return [cols[idx] for idx in xrange(len(cols)) if idx in colrange]
        return cols

    def base_cols(self):
        return self.mapping.keys()


    def get_attr_mappings(self):
        """
        create dictionary of base attribute name (dgnscd) to
        list of attribute names related (dgnscd1, dgnscd2,...)
        """
        mapping = defaultdict(list)
        with self.db.begin() as conn:
            cursor = self.db.execute("select * from %s limit 1" % self.table)
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





if __name__ == '__main__':
    db = create_engine('postgresql://localhost/penispros')
    hm = HistogramMaker(db, 'inp')
#    hm = HistogramMaker(db, 'prov_diag_counts')
#    res = hm('testies', ('provider',), ('dgnscd1',), [('sum', 'count')], [1])
    res = hm('testies', ('provider',), ('dgnscd',), [('count', 'id')], range(2,10))

    for x,y in res.items():
        print x, y
#    hm('diagnoses', ['provider', 'dgnscd'])
#    hm('diagnoses_norm', ['provider'])
