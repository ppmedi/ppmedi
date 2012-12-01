from histograms.util import HistogramMaker
from process.process_distributions import top_stats

if __name__ == "__main__":
  hm = HistogramMaker(DB, 'inp')
  top_stats(hm, 'prov_diag_counts', ('provider',), ('dgnscd1',) , 'count', range(2,10))


