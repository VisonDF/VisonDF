#pragma once

template <unsigned int CORES = 4,
          bool MemClean = false,
          bool SmallProportion = false,
          bool Soft = true>
void transform_filter_range_mt(std::vector<uint8_t>& mask,
                               const size_t strt_vl) 
{
  
    std::vector<uint8_t> x(mask.size, 0);

    if constexpr (SmallProportion) {
        size_t i2 = 0;
        for (size_t i = 0; i < mask.size(); ++i) {
            if (!mask[i]) {
                x[i2] = strt_vl + i;
                i2 += 1;
            }
        }
        rm_row_range_mt<CORES, 
                        MemClean,
                        Soft>(x);
    } else {
        #pragma omp parallel for if(CORES > 1) num_threads(CORES)
        for (size_t i = 0; i < mask.size(); ++i)
            x[i] = !mask[i];
        rm_row_range_reconstruct_boolmask_mt<CORES,
                                             MemClean,
                                             Soft>(x, strt_vl);
    }

};






