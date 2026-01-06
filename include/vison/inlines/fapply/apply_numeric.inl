#pragma once

template <bool MapCol = false,
          unsigned int CORES = 4,
          bool NUMA = false,
          typename T, 
          typename F>
inline void apply_numeric(const std::vector<std::vector<T>>& values, 
                          const unsigned int n, 
                          const size_t idx_type, 
                          F&& f,
                          const unsigned int strt
                          const unsigned int end) {
    
    unsigned int i2;
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

        const unsigned int n_el = end - strt;

        if (CORES > n_el)
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
                        n_el, 
                        tid, 
                        nthreads, 
                        numa_nodes);
            } else {
                simple_mt(cur_struct,
                          n_el, 
                          tid, 
                          nthreads);
            }
                
            const unsigned int cur_start = cur_struct.start;
            const unsigned int cur_end   = cur_struct.end;

            for (size_t i = cur_strt; i < cur_end; ++i)
                f(dst[i]);

        }

    } else {
        for (size_t i = strt; i < end; ++i)
            f(dst[i]);
    }

}





