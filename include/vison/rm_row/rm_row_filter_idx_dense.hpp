#pragma once

template <bool Sorted                  = true,
          bool MemClean                = false,
          bool Soft                    = true,
          bool Sorted                  = false,  // if not, it will modify x
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
                              Sorted,
                              IdxIsTrue,
                              AssertionLevel
                              >(x);

}


