#pragma once

template <typename T, bool IsBool = false> 
void rep_col_filter(std::vector<T>& x, 
                    unsigned int& colnb,
                    const std::vector<uint8_t>& mask)
{

    rep_col_filter_boolmask(x,
                            colnb,
                            mask,
                            0);

}


