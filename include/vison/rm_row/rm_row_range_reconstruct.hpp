#pragma once

template <bool Sorted   = true,
          bool MemClean = false,
          bool Soft = true,
          bool SanityCheck = true,
          bool Enforce = false  // let x as it is
         >
void rm_row_range_reconstruct(std::vector<unsigned int>& x)
{

   rm_row_range_reconstruct_mt<1, 
                               Sorted, 
                               MemClean,
                               Soft,
                               SanityCheck,
                               Enforce>(x);

}


