#pragma once

template <unsigned int CORES           = 4,
          bool NUMA                    = false,
          bool IsBool                  = false,
          bool MapCol                  = false,
          bool OneIsTrue               = true,
          bool Periodic                = false,
          AssertionType AssertionLevel = AssertionType::Simple,
          typename F,
          typename U
         >
requires span_or_vec<U>
requires FapplyFn<F, first_arg_t<F>>
void fapply_filter_mt(
                        F f, 
                        unsigned int n, 
                        const U& mask,
                        OffsetBoolMask& start_offset,
                        const unsigned int periodic_mask_len
                     ) 
{

    fapply_filter_range_mt<CORES,
                           NUMA,
                           IsBool, 
                           MapCol,
                           OneIsTrue,
                           Periodic,
                           AssertionLevel>(
           f,
           n,
           mask,
           0,   // strt_vl
           start_offset,
           periodic_mask_len
    ); 

}

template <unsigned int CORES           = 4,
          bool NUMA                    = false,
          bool IsBool                  = false,
          bool MapCol                  = false,
          bool OneIsTrue               = true,
          bool Periodic                = false,
          AssertionType AssertionLevel = AssertionType::Simple,
          typename F,
          typename U
         >
requires span_or_vec<U>
requires FapplyFn<F, first_arg_t<F>>
void fapply_filter_mt(
                        F f, 
                        unsigned int n, 
                        const U& mask,
                        OffsetBoolMask& start_offset = default_offset_start
                     ) 
{

    fapply_filter_range_mt<CORES,
                           NUMA,
                           IsBool, 
                           MapCol,
                           OneIsTrue,
                           Periodic,
                           AssertionLevel>(
        f,
        n,
        mask,
        0,   // strt_vl
        start_offset,
        nrow
    ); 
}







