#pragma once

template <bool MemClean = false,
          bool Soft = true>
void rm_row_range(std::vector<unsigned int>& x) 
{

    rm_row_range_mt<1, // CORES
                    MemClean,
                    Soft>(x);

};




