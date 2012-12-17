from collections import defaultdict
from histograms.util import HistogramMaker

import numpy
import scipy.stats

ICD_FILES = "../data/icd9_codes.txt"
SUMMARY_FUNCS = {
  #    'gini':,
  'stdev': numpy.std,
  'kurtosis': scipy.stats.kurtosis,
  'skew': scipy.stats.skew,
}

def get_distributions(table, join_cols, agg_group_cols, aggregate_col):
  """
  Example arguments
  table: prov_diag_counts
  join_cols: [provider]
  agg_group_cols: [diagnosis]
  aggregate_col: count

  Generates the following query
  SELECT stats.provider, stats.diagnosis, stats.count::float/normalize.normalize_sum AS freq
  FROM prov_diag_counts AS stats,
       (SELECT provider, SUM(count) AS normalize_sum FROM prov_diag_counts GROUP BY provider) as normalize
  WHERE stats.provider = normalize.provider;

  Returns {(diagnosis, ): [freq1, ..., freqN]}
  """
  normalize_query = "SELECT %s, SUM(%s) AS normalize_sum FROM %s GROUP BY %s" % (
    ", ".join(join_cols), aggregate_col, table, ", ".join(join_cols))
  from_clause = "FROM %s AS stats,\n\t(%s) AS normalize" % (table, normalize_query)
  join_conditions = ["stats.%s = normalize.%s" % (col, col) for col in join_cols]
  where_clause = "WHERE %s" % (", ".join(join_conditions))
  select_clause = "SELECT %s, stats.%s::float/normalize.normalize_sum AS freq " % (
    ", ".join("stats.%s" % (col) for col in agg_group_cols), aggregate_col)
  query = "%s\n%s\n%s" % (select_clause, from_clause, where_clause)
  with DB.begin() as conn:
    res = DB.execute(query).fetchall()
    distributions = defaultdict(list)
    for row in res:
      group = tuple(str(row[col]) for col in agg_group_cols)
      distributions[group].append(row['freq'])
  return distributions

def get_comorbidity_mappings():
  with open(ICD_FILES, 'r') as file:
    mappings = defaultdict(lambda:"???????")
    for line in file:
      parts = filter(bool, line.split("   "))
      if len(parts) != 3:
        continue
      mappings[parts[0]] = parts[2].strip()
    return mappings

def get_mappings(group):
  mappings = []
  como_mappings = get_comorbidity_mappings()
  def noop(x): return x
  def como_map(x): return como_mappings[x]

  for item in group:
    if item in {'diagnosis', 'dgnscd1'} or item.startswith('dgnscd'):
      mappings.append(como_map)
    else:
      mappings.append(noop)
  return mappings

def summarize(distribution):
  return {key: func(distribution) for key, func in SUMMARY_FUNCS.iteritems()}

def summarize_distributions(distributions):
  summaries = []
  for name, distribution in distributions.iteritems():
    if len(distribution) < 2:
      continue
    summary = summarize(distribution)
    summary['group'] = name
    summaries.append(summary)
  return summaries

def apply_mappings(mappings, group):
  return tuple(mapper(item) for mapper, item in zip(mappings, group))

def print_n_worst(summaries, mappings, n, key):
  sorted_summaries = sorted(summaries, reverse=True, key=lambda x: x[key])
  print "sorted by %s" % (key)
  print "%s\t%s\t%s\t%s" % ('index', 'diagnosis_id', key, 'group')
  for idx, summary in enumerate(sorted_summaries[:n]):
    print "%d\t%s\t%f\t%s" % (idx, summary['group'], summary[key],
                              apply_mappings(mappings, summary['group']))
  print "\n\n\n"

def top_stats(hm, table, join_cols, agg_group_cols, aggregate_col, ranges, N=50):
  """
  Prints the top N highest groups for each statistic in SUMMARY_FUNCS.
  Arguments are the same as those in get_distributions
  """
  distributions = hm(table, join_cols, agg_group_cols, aggregate_col, ranges)
  mappings = get_mappings(agg_group_cols)
  summaries = summarize_distributions(distributions)
  print "TABLE:", table
  for stat in SUMMARY_FUNCS.iterkeys():
    print_n_worst(summaries, mappings, N, stat)

if __name__ == "__main__":
  hm = HistogramMaker(DB, 'inp')
  top_stats(hm, 'prov_diag_counts', ('provider',), ('dgnscd1',) , 'count', range(2,10))


