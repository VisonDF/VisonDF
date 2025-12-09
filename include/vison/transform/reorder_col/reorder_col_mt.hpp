#pragma once

template <unsigned int CORES = 4,
          bool SanityCheck = false>
void reorder_col_mt(const std::vector<std::pair<unsigned int, unsigned int>>& swaps)
{

    if constexpr (SanityCheck) {
        ankerl::unordered_dense::set<unsigned> seen;
        seen.reserve(swaps.size() * 2);
        for (auto& [k, v] : swaps) {
            if (seen.contains(k) || seen.contains(v)) {
                std::cerr << "Cols must be unique among all swap pairs\n";
                return;
            }
            seen.insert(k);
            seen.insert(v);
        }
    }

    ankerl::unordered_dense::map<unsigned int, std::pair<size_t,size_t>> pos_of;
    pos_of.reserve(ncol);

    for (size_t i = 0; i < 6; ++i) {
        const unsigned int local_matr_size = matr_idx[i].size();
        for (size_t j = 0; j < local_matr_size; ++j)
            pos_of[matr_idx[i][j]] = {i, j};
    }

    #pragma omp parallel for if(CORES > 1) num_threads(CORES)
    for (size_t k = 0; k < swaps.size(); ++k) {

        const auto& [old_pos, new_pos] = swaps[k];

        assert(old_pos < ncol && "old_pos out of bounds");
        assert(new_pos < ncol && "new_pos out of bounds");

        std::swap(type_refv[old_pos], type_refv[new_pos]);
        std::swap(tmp_val_refv[old_pos], tmp_val_refv[new_pos]);

        if (!name_v.empty())
            std::swap(name_v[old_pos], name_v[new_pos]);

        const auto& loc1 = pos_of[old_pos];
        const auto& loc2 = pos_of[new_pos];

        std::swap(matr_idx[loc1.first][loc1.second],
                  matr_idx[loc2.first][loc2.second]);
    }

}



