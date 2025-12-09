#pragma once

template <bool Sorted   = true,
          bool MemClean = false,
          bool Soft = true,
          bool SanityCheck = true>
void rm_row_range_reconstruct_boolmask(std::vector<unsigned int>& x,
                                       const size_t strt_vl)
{

   rm_row_range_reconstruct_boolmask_mt<1, 
                                        Sorted, 
                                        MemClean,
                                        Soft,
                                        SanityCheck>(x, 
                                                     strt_vl);

}


