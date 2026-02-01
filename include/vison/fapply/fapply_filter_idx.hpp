#pragma once

template <bool IsBool                  = false,
          bool MapCol                  = false,
          bool IdxIsTrue               = true,
          bool Periodic                = false,
          AssertionType AssertionLevel = AssertionType::Simple,
          typename U,
          typename F>
requires span_or_vec<U>
requires FapplyFn<F, first_arg_t<F>>
void fapply_filter_idx(
                         F f, 
                         const unsigned int n, 
                         const U& mask,
                         const unsigned int periodic_mask_len
                      ) 
{

    fapply_filter_idx_mt<1,     // CORES
                         false, // NUMA locality
                         IsBool,
                         MapCol,
                         IdxIsTrue,
                         Periodic,
                         AssertionLevel
                         >(
       f,
       n,
       mask,
       periodic_mask_len
    );

}

template <bool IsBool                  = false,
          bool MapCol                  = false,
          bool IdxIsTrue               = true,
          bool Periodic                = false,
          AssertionType AssertionLevel = AssertionType::Simple,
          typename U,
          typename F>
requires span_or_vec<U>
requires FapplyFn<F, first_arg_t<F>>
void fapply_filter_idx(
                         F f, 
                         const unsigned int n, 
                         const U& mask
                      ) 
{

    fapply_filter_idx_mt<1,     // CORES
                         false, // NUMA locality
                         IsBool,
                         MapCol,
                         IdxIsTrue,
                         Periodic,
                         AssertionLevel
                         >(
       f,
       n,
       mask,
       nrow
    );

}





