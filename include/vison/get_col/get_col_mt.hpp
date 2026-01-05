#pragma once

template <unsigned int CORES = 4,
          bool IsBool = false,
          bool MapCol = false,
          typename T>
void get_col_mt(const unsigned int x, 
                std::vector<T> &rtn_v) {

    const unsigned int local_nrow = nrow;
    get_col_range_mt<CORES, IsBool, MapCol>(x, 
                                            rtn_v,
                                            0, // strt
                                            local_nrow);

};



