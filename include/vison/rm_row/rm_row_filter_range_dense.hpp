#pragma once

template <bool MemClean                = false,
          bool Soft                    = true,
          bool OneIsTrue               = true,
          bool Periodic                = false,
          AssertionType AssertionLevel = AssertionType::Simple
         >
void rm_row_filter_range_dense(std::vector<unsigned int>& x,
                               const size_t strt_vl)
{

   rm_row_filter_range_dense_mt<1,     // CORES
                                false, // NUMA locality
                                MemClean,
                                Soft,
                                OneIsTrue,
                                Periodic,
                                AssertionLevel>(x, 
                                                strt_vl);

}


