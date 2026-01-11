#pragma once

template <bool IsBool = false,
          bool MapCol = false,
          bool IsDense = false,
          bool OneIsTrue = true,
          typename T
        >
void get_col_filter_range(
                          const unsigned int x,
                          std::vector<T> &rtn_v,
                          const std::vector<uint8_t> &mask,
                          const unsigned int strt_vl,
                          std::vector<RunsIdxMt>& runs = {},
                          OffsetBoolMask& offset_start = {}
                         )
{

    get_col_filter_range_mt<1,     // CORES
                            false, // NUMA locality
                            IsBool,
                            MapCol,
                            IsDense,
                            OneIsTrue>(x, 
                                       rtn_v, 
                                       mask, 
                                       strt_vl,
                                       runs,
                                       offset_start);

}



