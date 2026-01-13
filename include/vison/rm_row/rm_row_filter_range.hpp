#pragma once

template <
          MtMethod MtType = MtMethod::Row,
          bool MemClean = false,
          bool Soft = true,
          bool OneIsTrue = true
         >
void rm_row_filter_range(std::vector<uint8_t>& x,
                         const size_t strt_vl)
{
    rm_row_filter_range_mt<1,     // CORES
                           false, // NUMA locality 
                           MtType,
                           MemClean,
                           Soft,
                           OneIsTrue>(x, strt_vl);
}

