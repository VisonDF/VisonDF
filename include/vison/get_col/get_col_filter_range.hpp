#pragma once

template <bool MemClean = false,
          bool IsBool = false,
          bool MapCol = false,
          bool IsDense = false,
          typename T
        >
void get_col_filter_range(unsigned int x,
                          std::vector<T> &rtn_v,
                          const std::vector<uint8_t> &mask,
                          const unsigned int strt_vl)
{

    get_col_filter_range_mt<1, // CORES
                            false, // NUMA locality
                            MemClean,
                            IsBool,
                            MapCol,
                            IsDense>(x, rtn_v, mask, strt_vl);

}



