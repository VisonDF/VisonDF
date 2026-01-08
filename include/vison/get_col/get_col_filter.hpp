#pragma once

template <bool IsBool = false,
          bool MapCol = false,
          bool IsDense = false,
          typename T
         >
void get_col_filter(
                    const unsigned int x,
                    std::vector<T> &rtn_v,
                    const std::vector<uint8_t> &mask,
                    std::vector<RunsIdxMt>& runs = {},
                    OffsetBoolMask& offset_start = {}
                   )
{

    get_col_filter_range_mt<1,     // CORES
                            false, // NUMA locality
                            IsBool,
                            MapCol,
                            IsDense>(x,
                                    rtn_v,
                                    mask,
                                    0,  // strt_vl
                                    runs,
                                    offset_start);
}



