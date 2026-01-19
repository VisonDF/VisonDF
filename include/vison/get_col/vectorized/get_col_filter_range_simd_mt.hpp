#pragma once

template <unsigned int CORES = 4,
          bool NUMA          = false,
          bool IsBool        = false,
          bool MapCol        = false,
          typename T>
void get_col_filter_range_simd_mt(const unsigned int x, 
                                  std::vector<T>& rtn_v,
                                  const std::vector<uint8_t>& mask,
                                  const unsigned int strt_vl) 
{

    const unsigned int n_el = mask.size();
  
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

    size_t active_count = 0;
    std::vector<size_t> pre_active_rows;

    if constexpr (CORES == 1) {
        for (size_t i = 0; i < n_el; ++i)
            active_count += mask[i] != 0;
    } else {
        pre_active_rows.resize(n_el, 0);
        for (size_t i = 0; i < n_el; ++i) {
            active_count += mask[i] != 0;
            pre_active_rows[i] = active_count;
        }
    }
    rtn_v.resize(active_count);

    int numa_nodes = 1;
    if (numa_available() >= 0)
        numa_nodes = numa_max_node() + 1;

    assert(CORES >= numa_nodes);
    assert(CORES % numa_nodes == 0);

    auto apply_filter = [&rtn_v, &mask, n_el, 
                         numa_nodes, strt_vl]<typename TB>(const std::vector<TB>& vec) {

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
            
                size_t out_idx = pre_active_rows[start];
            
                dispatch_get_filtered_col<sizeof(TB)>(vec, 
                                                      rtn_v,
                                                      mask,
                                                      strt_vl,
                                                      start,
                                                      end,
                                                      out_idx);
            }

        } else {

            dispatch_get_filtered_col<sizeof(TB)>(vec, 
                                                  rtn_v,
                                                  mask,
                                                  strt_vl,
                                                  0,    // start
                                                  n_el, // end
                                                  0     // out_idx
                                                  );

        }

    }

    if constexpr (std::is_same_v<T, std::string>)

        const size_t pos_base = find_col_base(matr_idx[0], 0);
        const std::vector<std::string>& src = str_v[pos_idx];

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
            
                size_t out_idx = pre_active_rows[start];
            
                for (size_t i = start; i < end; ++i) {
                    if (mask[i]) {
                        rtn_v[out_idx++] = src[strt_vl + i];
                    }
                }
            }

        } else {

            size_t out_idx = 0;
            for (size_t i = 0; i < n_el; ++i) {
                if (!mask[i]) { continue; }
                rtn_v[out_idx++] = src[strt_vl + i];
            }

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
            
                size_t out_idx = pre_active_rows[start];
            
                for (size_t i = start; i < end; ++i) {
                    if (!mask[i]) { continue; }
                    memcpy(rtn_v[out_idx++].data(), 
                           src[strt_vl + i].data(), 
                           sizeof(CharT));
                }
            }

        } else {

            size_t out_idx = 0;

            for (size_t i = 0; i < n_el; ++i) {
                if (!mask[i]) { continue; }
                memcpy(rtn_v[out_idx++].data(), 
                       src[strt_vl + i].data(), 
                       sizeof(CharT));
            }

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


