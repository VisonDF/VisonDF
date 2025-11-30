#pragma once

template <typename T, bool MemClean = false, bool IsBool = false>
void get_col_filter(unsigned int &x,
                    std::vector<T> &rtn_v,
                    const std::vector<uint8_t> &mask)
{

    get_col_filter_boolmask<T, MemClean, IsBool>(x,
                                                 rtn_v,
                                                 mask,
                                                 0,
                                                 nrow);
}



