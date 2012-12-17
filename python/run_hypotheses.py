from histograms.util import HistogramMaker
from process.process_distributions import top_stats
from sqlalchemy import create_engine

DB = create_engine("postgresql://localhost/penispros")#.connect()

if __name__ == "__main__":
  hm = HistogramMaker(DB, 'inp')
#  top_stats(hm, 'prov_diag_counts_1', ('provider',), ('dgnscd',) , [('count','*')], [0])
#  top_stats(hm, 'prov_diag_counts_2_9', ('provider',), ('dgnscd',) , [('count','*')], range(1,9))
#  top_stats(hm, 'prov_diag_counts_1_9', ('provider',), ('dgnscd',) , [('count','*')], range(0,9))
#  top_stats(hm, 'op_phys_diag_counts_1', ('op_npi',), ('dgnscd',) , [('count','*')], [0])
#  top_stats(hm, 'op_phys_diag_counts_2_9', ('op_npi',), ('dgnscd',) , [('count','*')], range(1,9))
#  top_stats(hm, 'op_phys_diag_counts_1_9', ('op_npi',), ('dgnscd',) , [('count','*')], range(0,9))
#
  top_stats(hm, 'prov_diag_sum_1', ('provider',), ('dgnscd',) , [('sum','tot_chrg')], [0])
  top_stats(hm, 'prov_diag_sum_2_9', ('provider',), ('dgnscd',) , [('sum','tot_chrg')], range(1,9))
  top_stats(hm, 'prov_diag_sum_1_9', ('provider',), ('dgnscd',) , [('sum','tot_chrg')], range(0,9))
  top_stats(hm, 'op_phys_diag_sum_1', ('op_npi',), ('dgnscd',) , [('sum','tot_chrg')], [0])
  top_stats(hm, 'op_phys_diag_sum_2_9', ('op_npi',), ('dgnscd',) , [('sum','tot_chrg')], range(1,9))
  top_stats(hm, 'op_phys_diag_sum_1_9', ('op_npi',), ('dgnscd',) , [('sum','tot_chrg')], range(0,9))

