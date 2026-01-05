#pragma once

template <bool MemClean = false, 
          bool IsBool = false,
          bool MapCol = false,
          typename T
        >
void get_col_filter_range(unsigned int x,
                          std::vector<T> &rtn_v,
                          const std::vector<uint8_t> &mask,
                          const unsigned int strt_vl)
{

    get_col_filter_range_mt<1, // CORES
                             MemClean,
                             IsBool,
                             MapCol>(x, rtn_v, mask, strt_vl);

}



