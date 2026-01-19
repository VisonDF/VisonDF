#pragma once

template <bool IsBool                  = false,
          bool MapCol                  = false,
          bool IsDense                 = false,
          bool IdxIsTrue               = true,
          bool Periodic                = true,
          AssertionType AssertionLevel = AssertionType::Normal
          typename T>
void get_col_filter_idx(unsigned int x,
                        std::vector<T> &rtn_v,
                        const std::vector<unsigned int> &mask,
                        Runs& runs = default_idx_runs)
{

    get_col_filter_idx_mt<1,      // CORES
                          false,  // NUMA locality
                          IsBool,
                          MapCol,
                          IsDense,
                          IdxIsTrue,
                          Periodic,
                          AssertionLevel>(x, 
                                          rtn_v, 
                                          mask,
                                          runs);

}




