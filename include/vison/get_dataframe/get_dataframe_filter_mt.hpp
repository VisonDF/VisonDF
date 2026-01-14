#pragma once

template <unsigned int CORES = 4,
          bool NUMA = false,
          bool IsDense = false,
          AssertionType AssertionLevel = AssertionType::Simple
         >
void get_dataframe_filter_mt(
                              const std::vector<size_t>& cols, 
                              Dataframe& cur_obj,
                              const std::vector<uint8_t>& mask,
                              OffsetBoolMask& offset_start = default_offset_start
                            )
{

    get_dataframe_filter_range_mt<CORES,   
                                  NUMA, 
                                  IsDense,
                                  AssertionLevel>(
                                  cols,
                                  cur_obj,
                                  mask,
                                  0,     // strt_vl
                                  runs,
                                  offset_start); 

}



