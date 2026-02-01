#pragma once

template <bool IsBool                  = false,
          bool MapCol                  = false,
          bool Isdense                 = false,
          bool OneIsTrue               = true,
          bool Periodic                = false,
          bool Move                    = false,
          AssertionType AssertionLevel = AssertionType::None,
          typename T,
          typename U
         > 
requires span_or_vec<T>
requires span_or_vec<U>
void rep_col_filter(
                    T& x, 
                    const unsigned int colnb,
                    const U& mask,
                    const unsigned int strt_vl,
                    OffsetBoolMask& offset_start = default_offset_start,
                    const unsigned int periodic_mask_len
                   )
{

    rep_col_filter_range_mt<1,     // CORES
                            false, // NUMA
                            IsBool,
                            MapCol,
                            IsDense,
                            OneIsTrue,
                            Periodic,
                            Move,
                            AssertionLevel>(
        x, 
        colnb,
        mask,
        0,  // strt_vl
        default_offset_start,
        periodic_mask_len
    );
}

template <bool IsBool                  = false,
          bool MapCol                  = false,
          bool Isdense                 = false,
          bool OneIsTrue               = true,
          bool Periodic                = false,
          bool Move                    = false,
          AssertionType AssertionLevel = AssertionType::None,
          typename T,
          typename U
         > 
requires span_or_vec<T>
requires span_or_vec<U>
void rep_col_filter_range(
                          T& x, 
                          const unsigned int colnb,
                          const U& mask,
                          const unsigned int strt_vl,
                          OffsetBoolMask& offset_start = default_offset_start,
                         )
{

    rep_col_filter_range_mt<1,     // CORES
                            false, // NUMA
                            IsBool,
                            MapCol,
                            IsDense,
                            OneIsTrue,
                            Periodic,
                            Move,
                            AssertionLevel>(
        x, 
        colnb,
        mask,
        0,  // strt_vl
        default_offset_start,
        (nrow - strt_vl)
    );
}





