#pragma once

template <typename F, bool IsBool = false>
requires FapplyFn<F, first_arg_t<F>>
void fapply_simd_filter(F f, 
                        unsigned int& n, 
                        const std::vector<uint8_t>& mask) 
{


    fapply_simd_filter_boolmask(f,
                                n,
                                mask,
                                0);

}



