#pragma once

template <bool Sorted   = true,
          bool MemClean = false>
void rm_row_range_reconstruct_boolmask(std::vector<unsigned int>& x)
{

   rm_row_range_reconstruct_boolmask_mt<1, 
                                        Sorted, 
                                        MemClean>(x);

}


