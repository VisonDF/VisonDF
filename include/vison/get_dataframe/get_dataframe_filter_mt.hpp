#pragma once

template <unsigned int CORES           = 4,
          bool NUMA                    = false,
          bool MapCol                  = false,
          bool IsDense                 = false,
          bool OneIsTrue               = true,
          bool Periodic                = false,
          AssertionType AssertionLevel = AssertionType::Simple,
          typename U
         >
requires span_or_vec<U>
void get_dataframe_filter_mt(
                              const std::vector<size_t>& cols, 
                              Dataframe& cur_obj,
                              const U& mask,
                              OffsetBoolMask& offset_start,
                              const unsigned int periodic_mask_len
                            )
{

    get_dataframe_filter_range_mt<CORES,   
                                  NUMA,
                                  MapCol,
                                  IsDense,
                                  OneIsTrue,
                                  Periodic,
                                  AssertionLevel>(
          cols,
          cur_obj,
          mask,
          0,     // strt_vl
          offset_start,
          periodic_mask_len
    ); 

}

template <unsigned int CORES           = 4,
          bool NUMA                    = false,
          bool MapCol                  = false,
          bool IsDense                 = false,
          bool OneIsTrue               = true,
          bool Periodic                = false,
          AssertionType AssertionLevel = AssertionType::Simple,
          typename U
         >
requires span_or_vec<U>
void get_dataframe_filter_mt(
                              const std::vector<size_t>& cols, 
                              Dataframe& cur_obj,
                              const U& mask,
                              OffsetBoolMask& offset_start = default_offset_start
                            )
{

    get_dataframe_filter_range_mt<CORES,   
                                  NUMA,
                                  MapCol,
                                  IsDense,
                                  OneIsTrue,
                                  Periodic,
                                  AssertionLevel>(
          cols,
          cur_obj,
          mask,
          0,     // strt_vl
          offset_start,
          nrow
    ); 

}



