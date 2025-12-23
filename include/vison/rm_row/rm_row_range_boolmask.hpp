#pragma once

template <bool MemClean = false,
          bool Soft = true>
void rm_row_range_boolmask(std::vector<unsigned int>& x)
{
    rm_row_range_boolmask_mt<1, //CORES
                             MemClean,
                             Soft>(x);
}

