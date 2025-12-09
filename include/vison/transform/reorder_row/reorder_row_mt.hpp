#pragma once

template <unsigned int CORES = 4,
          bool Soft = true,
          bool SanityCheck = false>
void reorder_row_mt(std::vector<std::pair<size_t, size_t>>& swaps) 
{

    if constexpr (SanityCheck) {
        std::unordered_set<size_t> seen;
        for (auto& [k, v] : swaps) {
            assert(!seen.count(k));
            assert(!seen.count(v));
            seen.insert(k);
            seen.insert(v);
        }
    }

    if constexpr (Soft) {

        in_view = true;
        const unsigned int local_nrow = nrow;
        #pragma omp parallel for if(CORES > 1) num_threads(CORES)
        for (size_t i = 0; i < local_nrow; ++i) {
            auto& [k, v] = swaps[i];
            std::swap(row_idx_view[k], row_idx_view[v]);
        }

    } else if constexpr (!Soft) {

        if (in_view) {
            std::cerr << "Can't perform this kind of operation while in `view_mode`, consider applying `.materialize()`\n"
            return;
        }

        swaper = [&]void (auto& vec) 
                  {
                       for (auto& [k, v] : swaps) {
                           std::swap(vec[k], vec[v]);
                       }
                  }

        unsigned int str_idx = 0, chr_idx =  0, bool_idx = 0;
        unsigned int int_idx = 0, uint_idx = 0, dbl_idx =  0;

        const unsigned int local_ncol = ncol;
        #pragma omp parallel for if(CORES > 1) num_threads(CORES)
        for (size_t t = 0; t < local_ncol; ++t) {
            switch(type_refv[t]) {
                case 's': swaper(str_v[str_idx]);   ++str_idx;  break;
                case 'c': swaper(chr_v[chr_idx]);   ++chr_idx;  break;
                case 'b': swaper(bool_v[bool_idx]); ++bool_idx; break;
                case 'i': swaper(int_v[int_idx]);   ++int_idx;  break;
                case 'u': swaper(uint_v[uint_idx]); ++uint_idx; break;
                case 'd': swaper(dbl_v[dbl_idx]);   ++dbl_idx;  break;
            }
        }

    }
}


