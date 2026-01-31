#pragma once

template <bool MemClean = false,
          bool Soft = false,
          AssertionType AssertionLevel = AssertionType::Simple
         >
void rm_row_range(const unsigned int start,
                  const unsigned int end) 
{

    rm_row_range_mt<1,     // CORES
                    false, // NUMA
                    MemClean,
                    Soft,
                    AssertionLevel>(start, end);

}
