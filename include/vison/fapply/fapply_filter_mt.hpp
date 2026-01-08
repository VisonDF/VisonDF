#pragma once

template <unsigned int CORES = 4,
          bool NUMA = false,
          bool IsBool = false,
          bool MapCol = false,
          typename F>
requires FapplyFn<F, first_arg_t<F>>
void fapply_filter_mt(F f, 
                      unsigned int n, 
                      const std::vector<uint8_t>& mask,
                      OffsetBoolMask& start_offset) 
{

    fapply_filter_range_mt<CORES,
                           NUMA,
                           IsBool, 
                           MapCol>(f,
                                   n,
                                   mask,
                                   0,   // strt_vl
                                   start_offset); 
}



