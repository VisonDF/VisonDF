#pragma once

template <bool MapCol = false,
          unsigned int CORES = 4,
          bool NUMA = false,
          bool IdxIsTrue = true,
          bool Sorted = true, // if no will modify mask if !IdxIsTrue
          typename T, 
          typename F>
inline void apply_numeric_filter_idx(const std::vector<T>& values, 
                                     const unsigned int n, 
                                     const size_t idx_type, 
                                     F&& f,
                                     const std::vector<unsigned int>& mask) 
{

    if constexpr (!Sorted && !IdxIsTrue) {
        std::sort(mask.begin(), mask.end());
    }

    unsigned int i2 = 0;
    if constexpr (!MapCol) {
        i2 = 0;
        while (i2 < matr_idx[idx_type].size()) {
            if (n == matr_idx[idx_type][i2])
                break;
            ++i2;
        }
    } else {
        if (!matr_idx_map[idx_type].contains(n)) {
            std::cerr << "MapCol used but col not found in the map\n";
            return;
        }
        if (!sync_map_col[idx_type]) {
            std::cerr << "Col not synced\n";
            return;
        }

        i2 = matr_idx_map[idx_type][n];

    }

    std::vector<T>& dst = values[i2];
    
    if constexpr (CORES > 1) {

        if (CORES > mask.size())
            throw std::runtime_error("Too much cores for so little nrows\n");

        int numa_nodes = 1;
        if (numa_available() >= 0) 
            numa_nodes = numa_max_node() + 1;

        #pragma omp prallel num_threads(CORES)
        {

            const int tid        = omp_get_thread_num();
            const int nthreads   = omp_get_num_threads();
           
            MtStruct cur_struct;

            if constexpr (NUMA) {
                numa_mt(cur_struct,
                        mask.size(), 
                        tid, 
                        nthreads, 
                        numa_nodes);
            } else {
                simple_mt(cur_struct,
                          mask.size(), 
                          tid, 
                          nthreads);
            }
                
            const unsigned int cur_start = cur_struct.start;
            const unsigned int cur_end   = cur_struct.end;

            if constexpr (IdxIsTrue) {

                for (size_t i = cur_start; i < cur_end; ++i)
                    f(dst[mask[i]]);

            } else {
                size_t out_idx = mask[cur_start - 1] + 1;
                size_t i       = cur_start;
                while (i < cur_end) {
                    while (out_idx < mask[i]) f(dst[out_idx++]);
                    out_idx += 1;
                }
            }

        }

    } else {
        if constexpr (IdxIsTrue) {
            for (auto i : mask)
                f(dst[i]);
        } else {
            size_t out_idx = 0;
            size_t i = 0;
            while (i < mask.size()) {
                while (out_idx < mask[i]) f(dst[out_idx++]);
                out_idx += 1;
            }
        }
    }

}




