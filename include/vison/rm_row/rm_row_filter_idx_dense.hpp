#pragma once

template <bool Sorted                  = true,
          bool MemClean                = false,
          bool Soft                    = true,
          bool IdxIsTrue               = true,
          AssertionType AssertionLevel = AssertionType::Normal
         >
void rm_row_filter_idx_dense(std::vector<unsigned int>& x)
{

   rm_row_filter_idx_dense_mt<1,     // CORES
                              false, // NUMA locality
                              Sorted, 
                              MemClean,
                              Soft,
                              IdxIsTrue,
                              AssertionLevel
                              >(x);

}


