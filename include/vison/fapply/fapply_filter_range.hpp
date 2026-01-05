#pragma once

template <bool IsBool = false,
          bool MapCol = false,
          typename F>
requires FapplyFn<F, first_arg_t<F>>
void fapply_filter_range(F f, 
                         const unsigned int n, 
                         const std::vector<uint8_t>& mask,
                         const unsigned int strt_vl) 
{

    fapply_filter_range_mt<IsBool, MapCol, 1>(f,
                                              n,
                                              mask,
                                              strt);

}



