from collections import defaultdict
from sqlalchemy import create_engine

import numpy
import scipy.stats


DBNAME = "penispros"
DB = create_engine("postgresql://localhost/%s" % DBNAME).connect()
QUERY = """
SELECT pdc.provider AS prov, pdc.dgnscd AS diag, pdc.count::float/pc.count AS freq
FROM prov_counts AS pc, diagnoses AS pdc
WHERE pc.provider::text = pdc.provider
"""
def get_distributions():
  with DB.begin() as conn:
    res = DB.execute(QUERY).fetchall()
    distributions = defaultdict(list)
    for row in res:
      distributions[str(row['diag'])].append(row['freq'])
  return distributions

def get_comorbidity_mappings():
  with open('data/icd9_codes.txt', 'r') as file:
    mappings = defaultdict(lambda:"???????")
    for line in file:
      parts = filter(bool, line.split("   "))
      if len(parts) != 3:
        continue
      mappings[parts[0]] = parts[2].strip()
    return mappings

def summarize(distribution):
  return {
#    'gini':,
    'stdev': numpy.std(distribution),
    'kurtosis': scipy.stats.kurtosis(distribution),
    'skew': scipy.stats.skew(distribution),
  }    

def summarize_distributions(distributions):
  summaries = []
  for name, distribution in distributions.iteritems():
    if len(distribution) < 2:
      continue
    summary = summarize(distribution)
    summary['name'] = name
    summaries.append(summary)
  return summaries

def print_n_worst(summaries, mappings, n, key):
  sorted_summaries = sorted(summaries, reverse=True, key=lambda x: x[key])
  print "sorted by %s" % (key)
  print "%s\t%s\t%s\t%s" % ('index', 'diagnosis_id', key, 'name')
  for idx, summary in enumerate(sorted_summaries[:n]):
    print "%d\t%s\t%f\t%s" % (idx, summary['name'], summary[key], mappings[summary['name']])
  print "\n\n\n"

N = 500
if __name__ == "__main__":
  distributions = get_distributions()
  mappings = get_comorbidity_mappings()
  summaries = summarize_distributions(distributions)
  print_n_worst(summaries, mappings, N, 'stdev')
  print_n_worst(summaries, mappings, N, 'skew')
  print_n_worst(summaries, mappings, N, 'kurtosis')
