#pragma once

template <
          bool IsBool = false,
          bool MapCol = false,
          typename T
         >
void get_col_filter_simd(const unsigned int x, 
                            std::vector<T> &rtn_v,
                            const std::vector<uint8_t> &mask) 
{

    get_col_filter_range_simd_mt<1, // CORES
                                 false, // NUMA locality
                                 IsBool, 
                                 MapCol>(x,
                                         rtn_v,
                                         mask,
                                         0); // strt_vl

};


