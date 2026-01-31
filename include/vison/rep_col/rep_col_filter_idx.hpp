#pragma once

template <unsigned int CORES           = 4,
          bool NUMA                    = false,
          bool IsBool                  = false,
          bool MapCol                  = false,
          bool IsDense                 = false,
          bool IdxIsTrue               = true,
          bool Periodic                = false,
          bool Move                    = false,  // side effects on mask, encouraged to be enabled is colnb corresponds to a string col
          AssertionType AssertionLevel = AssertionType::None,
          typename T,
          typename U> 
requires span_or_vec<T>
requires span_or_vec<U>
void rep_col_filter_idx_mt(
                           T& x, 
                           const unsigned int colnb,
                           const U& mask,
                           Runs& runs,
                           const unsigned int periodic_mask_len
                          )
{

    rep_col_filter_idx<1,       // CORES
                       false,   // NUMA
                       IsBool,
                       MapCol,
                       IsDense,
                       IdxIsTrue,
                       Periodic,
                       Move,
                       AssertionLevel>(
                                        x,
                                        colnb,
                                        mask,
                                        runs,
                                        nrow
                                      );

}

template <unsigned int CORES           = 4,
          bool NUMA                    = false,
          bool IsBool                  = false,
          bool MapCol                  = false,
          bool IsDense                 = false,
          bool IdxIsTrue               = true,
          bool Periodic                = false,
          bool Move                    = false,  // side effects on mask, encouraged to be enabled is colnb corresponds to a string col
          AssertionType AssertionLevel = AssertionType::None,
          typename T,
          typename U
         > 
requires span_or_vec<T>
requires span_or_vec<U>
void rep_col_filter_idx_mt(
                           T& x, 
                           const unsigned int colnb,
                           const U& mask,
                           Runs& runs = default_idx_runs
                          )
{

    rep_col_filter_idx<1,       // CORES
                       false,   // NUMA
                       IsBool,
                       MapCol,
                       IsDense,
                       IdxIsTrue,
                       Periodic,
                       Move,
                       AssertionLevel>(
                                        x,
                                        colnb,
                                        mask,
                                        runs,
                                        nrow
                                      );

}







