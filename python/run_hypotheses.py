from histograms.util import HistogramMaker
from process.process_distributions import top_stats
from sqlalchemy import create_engine

DB = create_engine("postgresql://localhost/penispros")#.connect()

if __name__ == "__main__":
  hm = HistogramMaker(DB, 'inp')
  top_stats(hm, 'prov_diag_counts', ('provider',), ('dgnscd',) , [('count','*')], range(2,10))


