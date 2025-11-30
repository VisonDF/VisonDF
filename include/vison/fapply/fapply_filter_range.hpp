#pragma once

template <typename F, bool IsBool = false>
requires FapplyFn<F, first_arg_t<F>>
void fapply_filter_range(F f, 
                         unsigned int& n, 
                         const std::vector<uint8_t>& mask,
                         const unsigned int strt_vl) 
{

    fapply_filter_boolmask<F, IsBool>(f,
                                      n,
                                      mask,
                                      strt_vl);

}



