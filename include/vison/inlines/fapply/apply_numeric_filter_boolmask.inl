#pragma once

template <bool MapCol        = false,
          unsigned int CORES = 4,
          bool OneIsTrue     = true,
          bool Periodic      = false,
          typename T, 
          typename F>
inline void apply_numeric_filter_boolmask(const std::vector<T>& values, 
                                          const unsigned int n, 
                                          const size_t idx_type, 
                                          F&& f,
                                          const std::vector<uint8_t>& mask,
                                          const unsigned int strt_vl,
                                          OffsetBoolMask& offset_start) 
{
    unsigned int idx_in_type = 0;
    if constexpr (!MapCol) {
        idx_in_type = 0;
        while (idx_in_type < matr_idx[idx_type].size()) {
            if (n == matr_idx[idx_type][idx_in_type])
                break;
            ++idx_in_type;
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

        idx_in_type = matr_idx_map[idx_type][n];

    }

    T* dst = values[idx_in_type].data() + strt_vl;
    const unsigned int n_el  = (Periodic) ? mask.size() : nrow - strt_vl;
    const unsigned int n_el2 = mask.size();

    if constexpr (CORES > 1) {

        if (CORES > mask.size())
            throw std::runtime_error("Too much cores for so little nrows\n");

        if (offset_start.vec.empty()) {
            boolmask_offset_per_thread<OneIsTrue,
                                       Periodic>(offset_start.thread_offsets, 
                                                 mask, 
                                                 CORES,
                                                 offset_start.active_rows,
                                                 n_el2);
        }

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

            const size_t out_idx = offset_start.thread_offsets[cur_start];

            if constexpr (!Periodic) {
                if constexpr (OneIsTrue) {
                    for (size_t i = cur_start; i < cur_end; ++i) {
                        if (!mask[i]) { continue; }
                        f(dst[out_idx]);
                        out_idx += 1;
                    }
                } else {
                    for (size_t i = cur_start; i < cur_end; ++i) {
                        if (mask[i]) { continue; }
                        f(dst[out_idx]);
                        out_idx += 1;
                    }
                }
            } else {
                if constexpr (OneIsTrue) {
                    for (size_t i = cur_start, k = cur_start % n_el2; i < cur_end; ++i) {
                        if (!mask[k]) { 
                            k += 1;
                            k -= (k == n_el2) * n_el2;
                            continue; 
                        }
                        f(dst[out_idx]);
                        out_idx += 1;
                    }
                } else {
                    for (size_t i = cur_start, k = cur_start % n_el2; i < cur_end; ++i) {
                        if (mask[k]) { 
                            k += 1;
                            k -= (k == n_el2) * n_el2;
                            continue; 
                        }
                        f(dst[out_idx]);
                        out_idx += 1;
                    }
                }
            }

        }

    } else {

        size_t out_idx = 0;

        if constexpr (!Periodic) {
            if constexpr (OneIsTrue) {
                for (size_t i = 0; i < n_el; ++i) {
                    if (!mask[i]) { continue; }
                    f(dst[out_idx]);
                    out_idx += 1;
                }
            } else {
                for (size_t i = 0; i < n_el; ++i) {
                    if (mask[i]) { continue; }
                    f(dst[out_idx]);
                    out_idx += 1;
                }
            }
        } else {
            if constexpr (OneIsTrue) {
                for (size_t i = 0, k = 0; i < n_el; ++i) {
                    if (!mask[k]) { 
                        k += 1;
                        k -= (k == n_el2) * n_el2;
                        continue; 
                    }
                    f(dst[out_idx]);
                    out_idx += 1;
                }
            } else {
                for (size_t i = 0, k = 0; i < n_el; ++i) {
                    if (mask[k]) { 
                        k += 1;
                        k -= (k == n_el2) * n_el2;
                        continue; 
                    }
                    f(dst[out_idx]);
                    out_idx += 1;
                }
            }
        }
    }
}







