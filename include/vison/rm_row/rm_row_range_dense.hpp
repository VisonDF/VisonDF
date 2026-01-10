#pragma once

template <bool Sorted   = true,
          bool MemClean = false,
          bool Soft = true,
          bool SanityCheck = true
         >
void rm_row_range_inplace(std::vector<unsigned int>& x)
{

   rm_row_range_inplace_mt<1,     // CORES
                           false, // NUMA locality
                           Sorted, 
                           MemClean,
                           Soft,
                           SanityCheck
                           >(x);

}


