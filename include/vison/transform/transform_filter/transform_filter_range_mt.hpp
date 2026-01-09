#pragma once

template <unsigned int CORES = 4,
          bool MemClean = false,
          bool SmallProportion = false,
          bool Soft = true>
void transform_filter_range_mt(std::vector<uint8_t>& mask,
                               const size_t strt_vl) 
{
  
    if constexpr (SmallProportion) {
        rm_row_range_mt<CORES, 
                        MemClean,
                        Soft>(x);
    } else {
        rm_row_range_reconstruct_boolmask_mt<CORES,
                                             MemClean,
                                             Soft>(x, strt_vl);
    }

};






