#pragma once

template <bool IsBool                  = false,
          bool MapCol                  = false,
          bool IsDense                 = false,
          bool IdxIsTrue               = true,
          bool Periodic                = true,
          AssertionType AssertionLevel = AssertionType::Normal
          typename T,
          typename U
         >
requires span_or_vec<U>
void get_col_filter_idx(unsigned int x,
                        std::vector<T> &rtn_v,
                        const U &mask,
                        Runs& runs,
                        const unsigned int periodic_mask_len)
{

    get_col_filter_idx_mt<1,      // CORES
                          false,  // NUMA locality
                          IsBool,
                          MapCol,
                          IsDense,
                          IdxIsTrue,
                          Periodic,
                          AssertionLevel>(
        x, 
        rtn_v, 
        mask,
        runs,
        periodic_mask_len
    );

}

template <bool IsBool                  = false,
          bool MapCol                  = false,
          bool IsDense                 = false,
          bool IdxIsTrue               = true,
          bool Periodic                = true,
          AssertionType AssertionLevel = AssertionType::Normal
          typename T,
          typename U
         >
requires span_or_vec<U>
void get_col_filter_idx(unsigned int x,
                        std::vector<T> &rtn_v,
                        const U &mask,
                        Runs& runs = default_idx_runs)
{

    get_col_filter_idx_mt<1,      // CORES
                          false,  // NUMA locality
                          IsBool,
                          MapCol,
                          IsDense,
                          IdxIsTrue,
                          Periodic,
                          AssertionLevel>(
        x, 
        rtn_v, 
        mask,
        runs,
        nrow
    );

}



