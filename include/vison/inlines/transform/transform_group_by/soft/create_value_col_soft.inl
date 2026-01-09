#pragma once

template <unsigned int CORES> 
inline void create_value_col_soft(auto& lookup,
                                  const size_t local_nrow,
                                 )
{
    if constexpr (CORES > 1) {
        
        std::vector<size_t> pos_boundaries;
        pos_boundaries.reserve(lookup.size() + 1);
        pos_boundaries.push_back(0);
        
        for (auto it = lookup.begin(); it != lookup.end(); ++it) {
            pos_boundaries.push_back(
                pos_boundaries.back() + it->second.idx_vec.size()
            );
        }
        
        auto it0 = lookup.begin();
        
        #pragma omp parallel for num_threads(CORES) schedule(static)
        for (size_t g = 0; g < lookup.size(); ++g) {

            size_t start = pos_boundaries[g];
            size_t len   = pos_boundaries[g + 1] - start;
            const std::vector<unsigned int>& vec = (it0 + g)->second.v;

            memcpy(row_view_idx.data() + start,
                   vec.data(),
                   len * sizeof(unsigned int));

        }

    } else {

        auto it = lookup.begin();
        size_t i2 = 0;

        for (size_t i = 0; i < lookup.size(); ++i) {

            const std::vector<unsigned int>& vec = (it + i)->second.v;

            memcpy(row_view_idx.data() + i2, 
                   vec.data(), 
                   sizeof(unsigned int) * vec.size());
            i2 += vec.size();

        }
    }
}



