#pragma once

template <bool MemClean = false,
          bool Soft = true>
void rm_row_filter_range_dense(std::vector<unsigned int>& x,
                               const size_t strt_vl)
{

   rm_row_filter_range_dense_mt<1,     // CORES
                                false, // NUMA locality
                                MemClean,
                                Soft>(x, 
                                      strt_vl);

}


