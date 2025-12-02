#pragma once

template<unsigned int CORES = 4, bool MemClean = false>
void transform_filter_mt(std::vector<uint8_t>& mask) 
{
    
    std::vector<unsigned int> to_delete;
    to_delete.resize(mask.size());

    size_t i2 = 0;
    for (size_t i = 0; i < mask.size(); ++i) {
        if (!mask[i]) {
            to_delete[i2] = i;
            i2 += 1;
        }
    }

    if (to_delete.size() / nrow < 0.08) {
        rm_row_range_mt<CORES, MemClean>(to_delete);
    } else {
        rm_row_range_reconstruct_mt<CORES>(to_delete);
    }

};





