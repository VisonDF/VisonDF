#pragma once

template <bool MemClean = false,
          bool Soft = true>
void rm_row_range_boolmask(std::vector<uint8_t>& x,
                           const size_t strt_vl)
{
    rm_row_range_boolmask_mt<1,     // CORES
                             false, // NUMA locality 
                             MemClean,
                             Soft>(x, strt_vl);
}

