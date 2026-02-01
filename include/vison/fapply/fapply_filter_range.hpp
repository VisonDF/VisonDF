#pragma once

template <bool IsBool                  = false,
          bool MapCol                  = false,
          bool OneIsTrue               = true,
          bool Periodic                = false,
          AssertionType AssertionLevel = AssertionType::Simple,
          typename F,
          typename U
         >
requires span_or_vec<U>
requires FapplyFn<F, first_arg_t<F>>
void fapply_filter_range(
                           F f, 
                           const unsigned int n, 
                           const U& mask,
                           const unsigned int strt_vl,
                           OffsetBoolMask& start_offset,
                           const unsigned int periodic_mask_len
                         ) 
{

    fapply_filter_range_mt<1,     // CORES
                           false, // NUMA locality
                           IsBool, 
                           MapCol,
                           OneIsTrue,
                           Periodic,
                           AssertionLevel>(
       f,
       n,
       mask,
       strt_vl,
       start_offset,
       periodic_mask_len
    );

}

template <bool IsBool                  = false,
          bool MapCol                  = false,
          bool OneIsTrue               = true,
          bool Periodic                = false,
          AssertionType AssertionLevel = AssertionType::Simple,
          typename F,
          typename U
         >
requires span_or_vec<U>
requires FapplyFn<F, first_arg_t<F>>
void fapply_filter_range(
                           F f, 
                           const unsigned int n, 
                           const U& mask,
                           const unsigned int strt_vl,
                           OffsetBoolMask& start_offset = default_offset_start
                         ) 
{

    fapply_filter_range_mt<1,     // CORES
                           false, // NUMA locality
                           IsBool, 
                           MapCol,
                           OneIsTrue,
                           Periodic,
                           AssertionLevel>(
       f,
       n,
       mask,
       strt_vl,
       start_offset,
       (nrow - strt_vl)
    );

}





