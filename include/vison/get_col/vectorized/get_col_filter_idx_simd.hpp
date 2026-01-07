#pragma once

template <
          unsigned int CORES = 4,
          bool NUMA = false,
          bool IsBool = false,
          bool MapCol = false,
          typename T
         >
void get_col_filter_idx_simd(const unsigned int x, 
                             std::vector<T> &rtn_v,
                             const std::vector<unsigned int> &mask) {
  
    const unsigned int n_el = mask.size();
    rtn_v.resize(n_el);

    int numa_nodes = 1;
    if (numa_available() >= 0)
        numa_nodes = numa_max_node() + 1;

    assert(CORES >= numa_nodes);
    assert(CORES % numa_nodes == 0);

    auto find_col_base = [x](const auto &idx_vec, [[maybe_unused]] const size_t idx_type) -> size_t {
        size_t pos;
        if constexpr (!MapCol) {
            pos = 0;
            while (pos < idx_vec.size() && idx_vec[pos] != x)
                ++pos;
  
            if (pos == idx_vec.size())
                throw std::runtime_error("Error in (get_col), no column found\n");
        } else {
            if (!matr_idx_map[idx_type].contains(x)) {
                throw std::runtime_error("MapCol chosen but col not found in map\n");
            }
            if (!sync_map_col[idx_type]) {
                throw std::runtime_error("Map not synced\n");
            }
            pos = matr_idx_map[idx_type][x];
        }
        return pos;
    };

    auto apply_filter = [&rtn_v, &mask, n_el, numa_nodes]<typename TB>(const std::vector<TB>& vec) {

        if constexpr (CORES > 1) {

            #pragma omp parallel num_threads(CORES)
            {
                const int tid        = omp_get_thread_num();
                const int nthreads   = omp_get_num_threads();
           
                MtStruct cur_struct;

                if constexpr (Numa) {
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
                    
                const unsigned int start = cur_struct.start;
                const unsigned int end   = cur_struct.end;

                dispatch_get_filtered_col_idx<sizeof(TB)>(vec,
                                                          rtn_v,
                                                          mask,
                                                          start,
                                                          end);

            }

        } else {

            dispatch_get_filtered_col_idx<sizeof(T)>(vec,
                                                     rtn_v,
                                                     mask,
                                                     0, // start
                                                     n_el);

        }

    };

    if constexpr (std::is_same_v<T, std::string>) {

        const size_t pos_base = find_col_base(matr_idx[0], 0);
        const std::vector<std::string>& src = str_v[pos_base];

        if constexpr (CORES > 1) {

            #pragma omp parallel num_threads(CORES)
            {
                const int tid        = omp_get_thread_num();
                const int nthreads   = omp_get_num_threads();
           
                MtStruct cur_struct;

                if constexpr (Numa) {
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
                    
                const unsigned int start = cur_struct.start;
                const unsigned int end   = cur_struct.end;

                for (size_t i = start; i < end; ++i)
                    rtn_v[i] = src[mask[i]];

            }

        } else {

            for (size_t i = 0; i < n_el; ++i)
                rtn_v[i] = src[mask[i]];

        }

    } else if constexpr (std::is_same_v<T, CharT>) {

        const size_t pos_base = find_col_base(matr_idx[1], 1);
        const std::vector<CharT>& src = chr_v[pos_base];

        if constexpr (CORES > 1) {

            #pragma omp parallel num_threads(CORES)
            {
                const int tid        = omp_get_thread_num();
                const int nthreads   = omp_get_num_threads();
           
                MtStruct cur_struct;

                if constexpr (Numa) {
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
                    
                const unsigned int start = cur_struct.start;
                const unsigned int end   = cur_struct.end;

                for (size_t i = start; i < end; ++i)
                    memcpy(rtn_v[i].data(),
                           src[mask[i]].data(),
                           sizeof(CharT));

            }

        } else {

            for (size_t i = 0; i < n_el; ++i)
                memcpy(rtn_v[i].data(),
                       src[mask[i]].data(),
                       sizeof(CharT));

        }

    } else if constexpr (IsBool) { 

        const size_t pos_base = find_col_base(matr_idx[2], 2);
        apply_filter(bool_v[pos_base]);

    } else if constexpr (std::is_same_v<T, IntT>) {

        const size_t pos_base = find_col_base(matr_idx[3], 3);
        apply_filter(int_v[pos_base]);

    } else if constexpr (std::is_same_v<T, UIntT>) {

        const size_t pos_base = find_col_base(matr_idx[4], 4);
        apply_filter(uint_v[pos_base]);

    } else if constexpr (std::is_same_v<T, FloatT>) {

        const size_t pos_base = find_col_base(matr_idx[5], 5);
        apply_filter(dbl_v[pos_base]);

    } else {

      throw std::runtime_error("Error in (get_col), unsupported type\n");

    };

};








