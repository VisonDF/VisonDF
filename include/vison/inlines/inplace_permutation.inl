#pragma once

template <typename T,
          unsigned int CORES = 4>
inline void inplace_permutation(std::vector<std::vector<T>>& v,
                                std::vector<unsigned int> perm)
{
    const size_t local_nrow = v[0].size();
    const size_t local_ncol = v.size();

    if constexpr (CORES > 1) {
        #pragma omp parallel for num_threads(CORES)
        for (size_t col = 0; col < local_ncol; ++col) {
 
	        if (std::find(col_alrd_materialized.begin(), 
	                 col_alrd_materialized.end(), col) != col_alrd_materialized.end())
		        continue;

            std::vector<size_t> perm2 = perm;
            auto& cur_v = v[col];

            for (size_t i = 0; i < local_nrow; ++i) {
                size_t current = i;
                while (perm2[current] != current) {
                    size_t next = perm2[current];
                    std::swap(cur_v[current], cur_v[next]);
                    std::swap(perm2[current], perm2[next]);
                }
            }
        }
    } else {
        std::vector<size_t> perm2(local_nrow);
        for (size_t col = 0; col < local_ncol; ++col) {
            memcpy(perm2.data(), 
                   perm.data(), 
                   local_nrow * sizeof(size_t)
                   );
            auto& cur_v = v[col];

            for (size_t i = 0; i < local_nrow; ++i) {
                size_t current = i;
                while (perm2[current] != current) {
                    size_t next = perm2[current];
                    std::swap(cur_v[current], cur_v[next]);
                    std::swap(perm2[current], perm2[next]);
                }
            }
        }
    }
    col_alrd_materialized.clear();
}






