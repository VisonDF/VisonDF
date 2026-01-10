#pragma once

template <
          MtMethod MtType = MtMethod::Row,
          bool MemClean = false,
          bool Soft = true,
          bool MakeAlloc = false,
          bool OneIsTrue = true
         >
void rm_row_range_boolmask(std::vector<uint8_t>& x,
                           const size_t strt_vl)
{
    rm_row_range_boolmask_mt<1,     // CORES
                             false, // NUMA locality 
                             MtType,
                             MemClean,
                             Soft,
                             MakeAlloc,
                             OneIsTrue>(x, strt_vl);
}

