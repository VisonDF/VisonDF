#pragma once

template <
          MtMethod MtType              = MtMethod::Row,
          bool MemClean                = false,
          bool Soft                    = true,
          bool OneIsTrue               = true,
          bool Perdiodic               = false,
          AssertionType AssertionLevel = AssertionType::Simple
         >
void rm_row_filter_range(
                         std::vector<uint8_t>& x,
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
                           AssertionLevel>(x, strt_vl, offset_start);
}

