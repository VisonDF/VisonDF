#pragma once

template <bool IsDense   = false,
          bool OneIsTrue = true>
void get_dataframe_filter_range(
                                const std::vector<size_t>& cols, 
                                Dataframe& cur_obj,
                                const std::vector<uint8_t>& mask,
                                const size_t strt_vl,
                                OffsetBoolMask& offset_start = default_offset_start
                                )
{

    get_dataframe_filter_range_mt<1,     // CORES
                                  false, // NUMA locality
                                  IsDense,
                                  OneIsTrue
                                 >(cols,
                                  cur_obj,
                                  mask,
                                  strt_vl,
                                  runs,
                                  offset_start); 

}



