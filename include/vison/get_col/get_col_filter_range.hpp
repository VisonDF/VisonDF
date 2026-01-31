#pragma once

template <bool IsBool                  = false,
          bool MapCol                  = false,
          bool IsDense                 = false,
          bool OneIsTrue               = true,
          bool Periodic                = false,
          AssertionType AssertionLevel = AssertionType::Simple,
          typename T,
          typename U
        >
requires span_or_vec<U>
void get_col_filter_range(
                          const unsigned int x,
                          std::vector<T> &rtn_v,
                          const U &mask,
                          const unsigned int strt_vl,
                          OffsetBoolMask& offset_start,
                          const unsigned int periodic_mask_len
                         )
{

    get_col_filter_range_mt<1,     // CORES
                            false, // NUMA locality
                            IsBool,
                            MapCol,
                            IsDense,
                            OneIsTrue,
                            Periodic,
                            AssertionLevel>(x, 
                                            rtn_v, 
                                            mask, 
                                            strt_vl,
                                            offset_start,
                                            periodic_mask_len);

}


template <bool IsBool                  = false,
          bool MapCol                  = false,
          bool IsDense                 = false,
          bool OneIsTrue               = true,
          bool Periodic                = false,
          AssertionType AssertionLevel = AssertionType::Simple,
          typename T,
          typename U
        >
requires span_or_vec<U>
void get_col_filter_range(
                          const unsigned int x,
                          std::vector<T> &rtn_v,
                          const U &mask,
                          const unsigned int strt_vl,
                          OffsetBoolMask& offset_start = default_offset_start
                         )
{

    get_col_filter_range_mt<1,     // CORES
                            false, // NUMA locality
                            IsBool,
                            MapCol,
                            IsDense,
                            OneIsTrue,
                            Periodic,
                            AssertionLevel>(
        x, 
        rtn_v, 
        mask, 
        strt_vl,
        offset_start,
        (nrow - strt_vl)
    );

}






