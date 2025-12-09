#pragma once

template<unsigned int CORES = 4, 
         bool MemClean = false,
         bool Soft = true>
void transform_filter_idx_mt(std::vector<unsigned int>& mask) 
{

    std::vector<uint8_t> used(nrow, 0);
    #pragma omp paralel for num_threads(CORES)
    for (size_t i = 0; i < mask.size(); ++i) {
        if (mask[i] < nrow) [[likely]]
            used[mask[i]] = 1;
    }

    std::vector<unsigned> to_delete;
    to_delete.resize(nrow + 1 - mask.size());
    size_t i2 = 0;
    for (unsigned int i = 0; i < nrow; ++i) {
        if (!used[i]) {
            to_delete[i2] = i;
            i2 += 1;
        }
    }

    if (to_delete.size() / nrow < 0.08) {
        rm_row_range_mt<CORES, 
                        MemClean,
                        Soft>(to_delete);
    } else {
        rm_row_range_reconstruct_mt<CORES, 
                                    true, 
                                    MemClean,
                                    Soft>(to_delete);
    }

};







