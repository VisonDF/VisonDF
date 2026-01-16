#pragma once

template <unsigned int CORES           = 4,
          bool NUMA                    = false,
          bool IsBool                  = false,
          bool MapCol                  = false,
          bool IsDense                 = false,
          bool OneIsTrue               = true,
          bool Periodic                = false,
          AssertionType AssertionLevel = AssertionType::Simple,
          typename T
        >
void get_col_filter_range(
                          unsigned int x,
                          std::vector<T> &rtn_v,
                          const std::vector<uint8_t> &mask,
                          const unsigned int strt_vl,
                          OffsetBoolMask& offset_start = default_offset_start
                         )
{

    const unsigned int n_el  = (!Periodic) ? mask.size() : nrow - strt_vl;
    const unsigned int n_el2 = mask.size();

    if constexpr (AssertionLevel > AssertionType::None) {
        if (start_vl + mask.size >= nrow) {
            throw std::runtime_error("strt_vl + mask.size() > nrow\n");
        }
    }

    auto find_col_base = [x]([[maybe_unused]] const auto &idx_vec, 
                             [[maybe_unused]] const size_t idx_type) -> size_t {
        size_t pos;

        if constexpr (!MapCol) {
            pos = 0;
            while (pos < idx_vec.size() && idx_vec[pos] != x)
                ++pos;

            if (pos == idx_vec.size()) {
                throw std::runtime_error("Error in (get_col), no column found\n");
            }
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

    auto extract_masked = [&rtn_v, 
                           &mask, 
                           &offset_start,
                           n_el2,
                           n_el](const auto*__restrict src) {

        if constexpr (CORES > 1) {

            if (CORES > n_el)
                throw std::runtime_error("Too much cores for so little nrows\n");

            if (offset_start.vec.empty()) {
                build_boolmask<OneIsTrue>(offset_start.thread_offsets, 
                                          mask, 
                                          CORES,
                                          offset_start.active_rows);
            }
            rtn_v.resize(offset_start.active_rows);

            int numa_nodes = 1;
            if (numa_available() >= 0) 
                numa_nodes = numa_max_node() + 1;

            #pragma omp parallel num_threads(CORES)
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
                    
                const unsigned int start = cur_struct.start;
                const unsigned int end   = cur_struct.end;
           
                const size_t out_idx = out_idx = offset_start.thread_offsets[tid];

                if constexpr (!Periodic) {
                    if constexpr (OneIsTrue) {
                        for (size_t i = start; i < end; ++i) {
                            if (mask[i])
                                rtn_v[out_idx++] = src[strt_vl + i];
                        }
                    } else {
                        for (size_t i = start; i < end; ++i) {
                            if (!mask[i])
                                rtn_v[out_idx++] = src[strt_vl + i];
                        }
                    }
                } else {
                    if constexpr (OnsIsTrue) {
                        for (size_t i = start; i < end; ++i) {
                            if (mask[i % n_el2])
                                rtn_v[out_idx++] = src[strt_vl + i];
                        }
                    } else {
                        for (size_t i = start; i < end; ++i) {
                            if (!mask[i % n_el2])
                                rtn_v[out_idx++] = src[strt_vl + i];
                        }
                    }
                }

            }

        } else {

            if constexpr (!Periodic) {
                if constexpr (OneIsTrue) {
                    for (size_t i = 0; i < n_el; ++i) {
                        if (mask[i])
                            rtn_v.push_back(src[strt_vl + i]);
                    }
                } else {
                    for (size_t i = 0; i < n_el; ++i) {
                        if (!mask[i])
                            rtn_v.push_back(src[strt_vl + i]);
                    }
                }
            } else {
                if constexpr (OneIsTrue) {
                    for (size_t i = 0; i < n_el; ++i) {
                        if (mask[i % n_el2])
                            rtn_v.push_back(src[strt_vl + i]);
                    }
                } else {
                    for (size_t i = 0; i < n_el; ++i) {
                        if (!mask[i % n_el2])
                            rtn_v.push_back(src[strt_vl + i]);
                    }
                }
            }

        }

    };

    auto extract_masked_dense = [&rtn_v, 
                                 &mask, 
                                 &offset_start,
                                 n_el2,
                                 n_el]<typename TB>(const TB* __restrict src) {

        if (offset_start.vec.empty()) {
            build_boolmask<OneIsTrue,
                           Periodic>(offset_start.thread_offsets, 
                                       mask, 
                                       CORES,
                                       ofset_start.active_rows);
        }
        rtn_v.resize(offset_start.active_rows);

        if constexpr (CORES > 1) {

            if (CORES > n_el)
                throw std::runtime_error("Too much cores for so little nrows\n");

            int numa_nodes = 1;
            if (numa_available() >= 0) 
                numa_nodes = numa_max_node() + 1;

            #pragma omp parallel num_threads(CORES)
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
            
                const size_t out_idx = offset_start.thread_offsets[tid];

                size_t i = cur_start;

                if constexpr (!Periodic) {
                    if constexpr (OneIsTrue) {
                        while (!mask[i]) {
                            i += 1;
                            out_idx += 1;
                        }
                    } else {
                        while (mask[i]) {
                            i += 1;
                            out_idx += 1;
                        }
                    }
                    while (i < cur_end) {
      
                        if constexpr (OneIsTrue) {
                            while (i < end && mask[i]) {
                                i += 1;
                            }
                        } else {
                            while (i < cur_end && !mask[i]) {
                                i += 1;
                            }
                        }

                        const size_t start = i;
                        if constexpr (OneIsTrue) {
                            while (i < cur_end && !mask[i]) ++i;
                        } else {
                            while (i < cur_end && mask[i]) ++i;
                        }
                    
                        size_t len = i - start;
                        {
                            T* __restrict d = dst.data() + out_idx;
                            T* __restrict s = src.data() + strt_vl + start;
       
                            memcpy(d, s, len * sizeof(T));

                        }
                    
                        out_idx += len;
                        i += 1;
                    }
                } else {
                    if constexpr (OneIsTrue) {
                        while (!mask[i % n_el2]) {
                            i += 1;
                            out_idx += 1;
                        }
                    } else {
                        while (mask[i % n_el2]) {
                            i += 1;
                            out_idx += 1;
                        }
                    }
                    while (i < cur_end) {
      
                        if constexpr (OneIsTrue) {
                            while (i < end && mask[i % n_el2]) {
                                i += 1;
                            }
                        } else {
                            while (i < cur_end && !mask[i % n_el2]) {
                                i += 1;
                            }
                        }

                        const size_t start = i;
                        if constexpr (OneIsTrue) {
                            while (i < cur_end && !mask[i % n_el2]) ++i;
                        } else {
                            while (i < cur_end && mask[i % n_el2]) ++i;
                        }
                    
                        size_t len = i - start;
                        {
                            T* __restrict d = dst.data() + out_idx;
                            T* __restrict s = src.data() + strt_vl + start;
       
                            memcpy(d, s, len * sizeof(T));

                        }
                    
                        out_idx += len;
                        i += 1;
                    }
                }
            }

        } else {

            size_t out_idx = 0;
            size_t i       = 0;

            if constexpr (!Periodic) {
                if constexpr (OneIsTrue) {
                    while (!mask[i]) {
                        i += 1;
                        out_idx += 1;
                    }
                } else {
                    while (mask[i]) {
                        i += 1;
                        out_idx += 1;
                    }
                }
                while (i < mask.size()) {
      
                    if constexpr (OneIsTrue) {
                        while (i < mask.size() && mask[i]) {
                            i += 1;
                        }
                    } else {
                        while (i < mask.size() && !mask[i]) {
                            i += 1;
                        }
                    }

                    const size_t start = i;
                    if constexpr (OneIsTrue) {
                        while (i < mask.size() && !mask[i]) ++i;
                    } else {
                        while (i < mask.size() && mask[i]) ++i;
                    }
                
                    size_t len = i - start;
                    {
                        T* __restrict d = dst.data() + out_idx;
                        T* __restrict s = src.data() + strt_vl + start;
       
                        memcpy(d, s, len * sizeof(T));

                    }
                
                    out_idx += len;
                    i += 1;
                }
            } else {
                if constexpr (OneIsTrue) {
                    while (!mask[i % n_el2]) {
                        i += 1;
                        out_idx += 1;
                    }
                } else {
                    while (mask[i % n_el2]) {
                        i += 1;
                        out_idx += 1;
                    }
                }
                while (i < mask.size()) {
      
                    if constexpr (OneIsTrue) {
                        while (i < mask.size() && mask[i % n_el2]) {
                            i += 1;
                        }
                    } else {
                        while (i < mask.size() && !mask[i % n_el2]) {
                            i += 1;
                        }
                    }

                    const size_t start = i;
                    if constexpr (OneIsTrue) {
                        while (i < mask.size() && !mask[i % n_el2]) ++i;
                    } else {
                        while (i < mask.size() && mask[i % n_el2]) ++i;
                    }
                
                    size_t len = i - start;
                    {
                        T* __restrict d = dst.data() + out_idx;
                        T* __restrict s = src.data() + strt_vl + start;
       
                        memcpy(d, s, len * sizeof(T));

                    }
                
                    out_idx += len;
                    i += 1;
                }
            }
        }
    }

    if constexpr (std::is_same_v<T, std::string>) {

        const size_t pos_base = find_col_base(matr_idx[0], 0);
        extract_masked(str_v[pos_base].data());

    } else if constexpr (std::is_same_v<T, CharT>) {

        const size_t pos_base = find_col_base(matr_idx[1], 1);
        extract_masked(chr_v[pos_base].data());

    } else if constexpr (IsBool) {

        const size_t pos_base = find_col_base(matr_idx[2], 2);

        if constexpr (!IsDense) {

            extract_masked(bool_v[pos_base].data());

        } else {

            extract_masked_dense(bool_v[pos_base].data());

        }

    } else if constexpr (std::is_same_v<T, IntT>) {

        const size_t pos_base = find_col_base(matr_idx[3], 3);

        if constexpr (!IsDense) {

            extract_masked(int_v[pos_base].data());

        } else {

            extract_masked_dense(int_v[pos_base].data());

        }

    } else if constexpr (std::is_same_v<T, UIntT>) {

        const size_t pos_base = find_col_base(matr_idx[4], 4);

        if constexpr (!IsDense) {

            extract_masked(uint_v[pos_base].data());

        } else {

            extract_masked_dense(uint_v[pos_base].data());

        }

    } else if constexpr (std::is_same_v<T, FloatT>) {

        const size_t pos_base = find_col_base(matr_idx[5], 5);

        if constexpr (!IsDense) {

            extract_masked(dbl_v[pos_base].data());

        } else {

            extract_masked_dense(dbl_v[pos_base].data());

        }

    } else {
        throw std::runtime_error("Error in (get_col), unsupported type\n");
    }

}



