#pragma once

template <unsigned int CORES = 4,
          bool MemClean = false, 
          bool IsBool = false,
          bool MapCol = false,
          typename T
         >
void get_col_filter_mt(unsigned int x,
                       std::vector<T> &rtn_v,
                       const std::vector<uint8_t> &mask)
{

    get_col_filter_range_mt<CORES,
                            MemClean, 
                            IsBool,
                            MapCol>(x,
                                    rtn_v,
                                    mask,
                                    0);
}



