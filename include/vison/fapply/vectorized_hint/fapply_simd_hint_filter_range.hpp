#pragma once

template <typename F, bool IsBool = false>
requires FapplyFn<F, first_arg_t<F>>
void fapply_simd_filter_range(F f, 
                              unsigned int& n, 
                              const std::vector<uint8_t>& mask,
                              const unsigned int strt_vl) 
{

    fapply_simd_filter_boolmask(f,
                                n,
                                mask,
                                strt_vl);

}



