#pragma once

template <bool IsBool = false,
          bool MapCol = false,
          bool IsDense = false,
          typename T>
void get_col_filter_idx(unsigned int x,
                        std::vector<T> &rtn_v,
                        const std::vector<unsigned int> &mask)
{

    get_col_filter_idx_mt<1, // CORES
                          IsBool,
                          MapCol,
                          IsDense>(x, rtn_v, mask);

}




