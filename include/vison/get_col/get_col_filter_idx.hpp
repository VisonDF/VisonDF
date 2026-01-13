#pragma once

template <bool IsBool    = false,
          bool MapCol    = false,
          bool IsDense   = false,
          bool IsSorted  = true,
          bool IdxIsTrue = true,
          typename T>
void get_col_filter_idx(unsigned int x,
                        std::vector<T> &rtn_v,
                        const std::vector<unsigned int> &mask,
                        Runs& runs = Runs{})
{

    get_col_filter_idx_mt<1,      // CORES
                          false,  // NUMA locality
                          IsBool,
                          MapCol,
                          IsDense,
                          IsSorted,
                          IdxIsTrue>(x, 
                                     rtn_v, 
                                     mask,
                                     runs);

}




