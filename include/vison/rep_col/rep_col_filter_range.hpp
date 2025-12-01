#pragma once

template <typename T, bool IsBool = false> 
void rep_col_filter_range(std::vector<T>& x, 
                          unsigned int& colnb,
                          const std::vector<uint8_t>& mask,
                          const unsigned int& strt_vl)
{

    rep_col_filter_boolmask(x,
                            colnb,
                            mask,
                            strt_vl);

}


