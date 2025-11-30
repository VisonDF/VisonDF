#pragma once

template <typename T, bool IsBool = false>
void get_col_filter_simd(unsigned int &x, 
                std::vector<T> &rtn_v,
                const std::vector<uint8_t> &mask) 
{

    get_col_filter_boolmask_simd<T, IsBool>(x,
                                            rtn_v,
                                            mask,
                                            0);

};


