#pragma once

template <bool Sorted   = true,
          bool MemClean = false>
void rm_row_range_reconstruct(std::vector<unsigned int>& x)
{

   rm_row_range_reconstruct_mt<1, 
                               Sorted, 
                               MemClean>(x);

}


