#pragma once

template <typename T, 
          bool MemClean = false, 
          bool IsBool = false>
void get_col_filter_range(unsigned int &x,
                          std::vector<T> &rtn_v,
                          const std::vector<uint8_t> &mask,
                          const unsigned int& strt_vl)
{
    get_col_filter_boolmask<T, MemClean, IsBool>(x,
                                                 rtn_v,
                                                 mask,
                                                 strt_vl,
                                                 nrow);
}



