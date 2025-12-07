#pragma once

template<bool MemClean = false>
void transform_filter_idx(std::vector<unsigned int>& mask) 
{

   std::vector<uint8_t> used(nrow, 0);
    for (unsigned x : mask) {
        if (x < nrow) [[likely]]
            used[x] = 1;
    }

    std::vector<unsigned> to_delete;
    to_delete.reserve(nrow + 1 - mask.size()); 
    for (unsigned i = 0; i < nrow; ++i) {
        if (!used[i])
            to_delete.push_back(i);
    }

    if (to_delete.size() / nrow < 0.08) {
        rm_row_range_mt<1, MemClean>(to_delete);
    } else {
        rm_row_range_reconstruct_mt<1, true, MemClean>(to_delete);
    }

};





