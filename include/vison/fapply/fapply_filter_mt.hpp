#pragma once

template <bool IsBool = false,
          bool MapCol = false,
          unsigned int CORES = 4,
          typename F>
requires FapplyFn<F, first_arg_t<F>>
void fapply_filter_mt(F f, 
                      unsigned int n, 
                      const std::vector<uint8_t>& mask) 
{

    fapply_filter_range_mt<IsBool, MapCol, CORES>(f,
                                                  n,
                                                  mask,
                                                  0);
}



