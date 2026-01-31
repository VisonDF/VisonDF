#pragma once

template <bool MapCol                  = false,
          bool IsDense                 = false,
          bool OneIsTrue               = true,
          bool Periodic                = false,
          AssertionType AssertionLevel = AssertionType::Simple,
          typename T
         >
requires span_or_vec<T>
void get_dataframe_filter_range(
                                const std::vector<size_t>& cols, 
                                Dataframe& cur_obj,
                                const T& mask,
                                const size_t strt_vl,
                                OffsetBoolMask& offset_start,
                                const unsigned int periodic_mask_len
                                )
{

    get_dataframe_filter_range_mt<1,     // CORES
                                  false, // NUMA locality
                                  MapCol,
                                  IsDense,
                                  OneIsTrue,
                                  Periodic,
                                  AssertionType
                                 >(
        cols,
        cur_obj,
        mask,
        strt_vl,
        offset_start,
        periodic_mask_len
    ); 

}


template <bool MapCol                  = false,
          bool IsDense                 = false,
          bool OneIsTrue               = true,
          bool Periodic                = false,
          AssertionType AssertionLevel = AssertionType::Simple,
          typename T
         >
requires span_or_vec<T>
void get_dataframe_filter_range(
                                const std::vector<size_t>& cols, 
                                Dataframe& cur_obj,
                                const T& mask,
                                const size_t strt_vl,
                                OffsetBoolMask& offset_start = default_offset_start,
                                )
{

    get_dataframe_filter_range_mt<1,     // CORES
                                  false, // NUMA locality
                                  MapCol,
                                  IsDense,
                                  OneIsTrue,
                                  Periodic,
                                  AssertionType
                                 >(
        cols,
        cur_obj,
        mask,
        strt_vl,
        offset_start,
        (cur_obj.get_nrow() - strt_vl)
    ); 

}




