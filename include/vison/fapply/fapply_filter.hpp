#pragma once

template <bool IsBool = false,
          bool MapCol = false,
          typename F>
requires FapplyFn<F, first_arg_t<F>>
void fapply_filter(
                     F f, 
                     unsigned int n, 
                     const std::vector<uint8_t>& mask,
                     OffsetBoolMask& start_offset
                  ) 
{

    fapply_filter_range_mt<1,     // CORES
                           false, // NUMA locality
                           IsBool, 
                           MapCol>(f,
                                   n,
                                   mask,
                                   0,  // strt_vl
                                   start_offset);
}



