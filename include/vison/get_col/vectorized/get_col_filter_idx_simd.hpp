#pragma once

template <
          bool IsBool = false,
          bool MapCol = false,
          typename T
         >
void get_col_filter_idx_simd(const unsigned int x, 
                             std::vector<T> &rtn_v,
                             const std::vector<unsigned int> &mask) 
{

    get_col_filter_idx_simd_mt<1, // CORES
                               false, // NUMA locality
                               IsBool,
                               MapCol>(x, 
                                       rtn_v, 
                                       mask);

};








