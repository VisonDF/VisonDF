#pragma once

template <bool IsDense = false>
void get_dataframe_filter(
                          const std::vector<size_t>& cols, 
                          Dataframe& cur_obj,
                          const std::vector<uint8_t>& mask,
                          std::vector<RunsIdxMt>& runs = {},
                          OffsetBoolMask& offset_start = {}
                         )
{

    get_dataframe_filter_range_mt<1,     // CORES
                                  false, // NUMA locality
                                  IsDense
                                 >(cols,
                                  cur_obj,
                                  mask,
                                  0,     // strt_vl
                                  runs,
                                  offset_start); 

}



