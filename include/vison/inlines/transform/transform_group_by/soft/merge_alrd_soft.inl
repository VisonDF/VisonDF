#pragma once

template <unsigned int CORES>
inline void merge_alrd_soft(
                           const size_t unique_grps,
                           const size_t chunks,
                           const std::vector<unsigned int>& grp_by,
                           std::vector<ReservingVec<unsigned int>>& val_grp,
                           const std::vector<std::vector<ReservingVec<unsigned int>>>& val_grp_vec
                           )
{
    #pragma omp prallel for if(CORES > 1) num_threads(CORES)
    for (size_t i = 0; i < unique_grps; ++i) {

        auto& vec = val_grp[i].v;
        size_t start = 0;

        for (size_t i2 = 0; i2 < chunks; ++i2) {
            const auto& cur_vec = val_grp_vec[i2][i].v;
            memcpy(vec.data() + start,
                   cur_vec.data(),
                   cur_vec.size() * sizeof(unsigned int));
            start += cur_vec.size();
        }
    }
}

