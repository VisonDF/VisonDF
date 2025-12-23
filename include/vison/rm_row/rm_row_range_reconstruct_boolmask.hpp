#pragma once

template <bool MemClean = false,
          bool Soft = true>
void rm_row_range_reconstruct_boolmask(std::vector<unsigned int>& x,
                                       const size_t strt_vl)
{

   rm_row_range_reconstruct_boolmask_mt<1, // CORES
                                        MemClean,
                                        Soft>(x, 
                                              strt_vl);

}


