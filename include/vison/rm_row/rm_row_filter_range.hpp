#pragma once

template <
          MtMethod MtType              = MtMethod::Row,
          bool MemClean                = false,
          bool Soft                    = true,
          bool OneIsTrue               = true,
          bool Perdiodic               = false,
          AssertionType AssertionLevel = AssertionType::Simple,
          typename T
         >
requires span_or_vec<T>
void rm_row_filter_range(
                         const T& mask,
                         const size_t strt_vl,
                         BoolMaskOffset& offset_start,
                         const unsigned int periodic_mask_len
                        )
{
    rm_row_filter_range_mt<1,     // CORES
                           false, // NUMA locality 
                           MtType,
                           MemClean,
                           Soft,
                           OneIsTrue,
                           Periodic,
                           AssertionLevel>(
        x, 
        strt_vl, 
        offset_start,
        periodic_mask_len
    );
}

template <
          MtMethod MtType              = MtMethod::Row,
          bool MemClean                = false,
          bool Soft                    = true,
          bool OneIsTrue               = true,
          bool Perdiodic               = false,
          AssertionType AssertionLevel = AssertionType::Simple,
          typename T
         >
requires span_or_vec<T>
void rm_row_filter_range(
                         const T& mask,
                         const size_t strt_vl,
                         BoolMaskOffset& offset_start = default_offset_start
                        )
{
    rm_row_filter_range_mt<1,     // CORES
                           false, // NUMA locality 
                           MtType,
                           MemClean,
                           Soft,
                           OneIsTrue,
                           Periodic,
                           AssertionLevel>(
        mask, 
        strt_vl, 
        offset_start,
        (nrow - strt_vl)
    );
}






