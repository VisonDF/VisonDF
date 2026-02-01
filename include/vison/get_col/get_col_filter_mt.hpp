#pragma once

template <unsigned int CORES           = 4,
          bool NUMA                    = false,
          bool IsBool                  = false,
          bool MapCol                  = false,
          bool IsDense                 = false,
          bool OneIsTrue               = true,
          bool Periodic                = false,
          AssertionType AssertionLevel = AssertionType::Simple,
          typename T,
          typename U
         >
requires span_or_vec<U>
void get_col_filter_mt(
                        const unsigned int x,
                        std::vector<T> &rtn_v,
                        const U &mask,
                        OffsetBoolMask& offset_start,
                        const unsigned int periodic_mask_len
                      )
{

    get_col_filter_range_mt<CORES,     
                            NUMA, 
                            IsBool,
                            MapCol,
                            IsDense,
                            OneIsTrue,
                            Periodic,
                            AssertionLevel>(
        x,
        rtn_v,
        mask,
        0,     // strt_vl
        runs,
        offset_start,
        periodic_mask_len
    );
}

template <unsigned int CORES           = 4,
          bool NUMA                    = false,
          bool IsBool                  = false,
          bool MapCol                  = false,
          bool IsDense                 = false,
          bool OneIsTrue               = true,
          bool Periodic                = false,
          AssertionType AssertionLevel = AssertionType::Simple,
          typename T,
          typename U
         >
requires span_or_vec<U>
void get_col_filter_mt(
                        const unsigned int x,
                        std::vector<T> &rtn_v,
                        const U &mask,
                        OffsetBoolMask& offset_start = default_offset_start
                      )
{

    get_col_filter_range_mt<CORES,     
                            NUMA, 
                            IsBool,
                            MapCol,
                            IsDense,
                            OneIsTrue,
                            Periodic,
                            AssertionLevel>(
        x,
        rtn_v,
        mask,
        0,     // strt_vl
        runs,
        offset_start,
        nrow
    );
}






