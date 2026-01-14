#pragma once

template <unsigned int CORES           = 4,
          bool NUMA                    = false,
          bool IsBool                  = false,
          bool MapCol                  = false,
          bool IsDense                 = false, // assumed sorted increasingly
          bool IsSorted                = true, 
          bool IdxIsTrue               = true,
          AssertionType AssertionLevel = AssertionType::Simple,
          typename T>
void get_col_filter_idx_mt(
                           unsigned int x,
                           std::vector<T> &rtn_v,
                           std::vector<unsigned int> &mask,
                           Runs& runs = default_idx_runs
                          )
{

    const unsigned int local_nrow = nrow;

    if constexpr (AssertionLevel > AssertionType::None) {
        if constexpr (IsDense && !Sorted) {
            throw std::runtime_error("To use `IsDense` parameter, you must sort the mask\n");
        }

        if constexpr (!IsSorted && !IdxIsTrue) {
            std::sort(mask.begin(), mask.end());
        }

        if constexpr (!IdxIsTrue) {
            if (mask.back() >= local_nrow)
                throw std::runtime_error("mask indices are exceeding nrow\n");
        }

        if constexpr (IdxIsTrue) {
            rtn_v.resize(mask.size());
        } else {
            rtn_v.resize(local_nrow - mask.size());
        }
    }

    if constexpr (AssertionLevel == AssertionType::Hard) {
        if constexpr (IdxIsTrue && IsDense) {
            const ref_val = mask[0];
            for (size_t i = 1; i < mask.size(); ++i) {
                if (ref_val < mask[i]) [[unlikely]] {
                    throw std::runtime_error("mask is not sorted ascendingly\n");
                }
            }
            if (mask.back() >= local_nrow) {
                throw std::runtime_error("mask indices are out of bound\n");
            }
        } else if constexpr (IdxIsTrue) {
            for (auto el : mask) {
                if (el >= local_nrow) {
                    throw std::runtime_error("mask indices are out of bound\n");
                }
            }
        }
    }

    auto find_col_base = [x](const auto &idx_vec, [[maybe_unused]] const size_t idx_type) -> size_t 
    {
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

    auto extract_idx_masked = [&rtn_v, &mask](const auto*__restrict src) {

        if constexpr (CORES > 1) {

            if (CORES > rtn_v.size())
                throw std::runtime_error("Too much cores for so little nrows\n");

            if constexpr (!IdxIsTrue) {
                if (runs.thread_offsets.empty()) { 
                    idx_offset_per_thread_mt_simple<IdxIsTrue>(runs.thread_offsets,
                                                               mask,
                                                               CORES);
                }
            }

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
                            rtn_v.size(), 
                            tid, 
                            nthreads, 
                            numa_nodes);
                } else {
                    simple_mt(cur_struct,
                              rtn_v.size(), 
                              tid, 
                              nthreads);
                }
                    
                const unsigned int start = cur_struct.start;
                const unsigned int end   = cur_struct.end;

                if constexpr (IdxIsTrue) {
                    for (size_t i = start; i < end; ++i)
                        rtn_v[i] = src[mask[i]];
                } else {
                    size_t out_idx = runs.thread_offsets[tid];
                    for (size_t i = start; i < end; ++i) {
                        const size_t next_stop = mask[i];
                        while (out_idx < next_stop) {
                            rtn_v[out_idx] = src[out_idx];
                            out_idx += 1;
                        }
                    }
                }

            }

        } else {

            if constexpr (IdxIsTrue) {
                for (size_t i = 0; i < mask.size(); ++i)
                    rtn_v[i] = src[mask[i]];
            } else {
                size_t out_idx = 0;
                for (size_t i = 0; i < mask.size(); ++i) {
                    const size_t next_stop = mask[i];
                    while (out_idx < next_stop) {
                        rtn_v[out_idx] = src[out_idx];
                        out_idx += 1;
                    }
                    out_idx += 1;
                }
            }
        }
    };

    auto extract_idx_masked_dense = [local_nrow,
                                     &mask]<typename T>(
                                                        T* __restrict dst,
                                                        const T* __restrict src,
                                                       ) 
    {

        if constexpr (CORES > 1) {

            if (CORES > rtn_v.size())
                throw std::runtime_error("Too much cores for so little nrows\n");

            std::vector<size_t> thread_counts;

            if (runs.vec.empty() || runs.thread_offsets.empty()) {

                idx_offset_per_thread_mt<IdxIsTrue>(runs.thread_offsets,
                                                    thread_counts,
                                                    mask,
                                                    CORES,
                                                    local_nrow,
                                                    runs.vec);

            }
           
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
                            runs.size(), 
                            tid, 
                            nthreads, 
                            numa_nodes);
                } else {
                    simple_mt(cur_struct,
                              runs.size(), 
                              tid, 
                              nthreads);
                }
                    
                const size_t start = cur_struct.start;
                const size_t len   = runs.thread_offsets[tid];

                if constexpr (IdxIsTrue) {
                    for (size_t r = start; r < start + len; ++r) {
                        const auto& run = runs.vec[r]; 
                        std::memcpy(dst.data() + run.mask_pos,
                                    src.data() + run.src_start,
                                    run.len * sizeof(T));
                    }
                } else {
                    const size_t delta = thread_counts[tid];
                    for (size_t r = start; r < start + len; ++r) {
                        const auto& run = runs[r]; 
                        std::memcpy(dst.data() + delta + run.mask_pos,
                                    src.data() + run.src_start,
                                    run.len * sizeof(T));
                    }
                }
            }

        } else {

            if (runs.empty()) {

                idx_offset_per_thread<IdxIsTrue>(mask,
                                                 local_nrow,
                                                 runs.vec);

            }

            for (size_t r = 0; r < runs.size(); ++r) {
                const auto& run = runs.vec[r]; 
                std::memcpy(dst.data() + run.mask_pos,
                            src.data() + run.src_start,
                            run.len * sizeof(T));
            }
        }
    }

    if constexpr (std::is_same_v<T, std::string>) {

        const size_t pos_base = find_col_base(matr_idx[0], 0);
        extract_idx_masked(str_v[pos_base].data());

    } else if constexpr (std::is_same_v<T, CharT>) {

        const size_t pos_base = find_col_base(matr_idx[1], 1);
        if constexpr (!IsDense) {
            extract_idx_masked(chr_v[pos_base].data());
        } else {
            extract_idx_masked_dense(chr_v[pos_base].data(), rtn_v.data());
        }

    } else if constexpr (IsBool) {

        const size_t pos_base = find_col_base(matr_idx[2], 2);
        if constexpr (!IsDense) {
            extract_idx_masked(bool_v[pos_base].data());
        } else {
            extract_idx_masked_dense(bool_v[pos_base].data(), rtn_v.data());
        }

    } else if constexpr (std::is_same_v<T, IntT>) {

        const size_t pos_base = find_col_base(matr_idx[3], 3);
        if constexpr (!IsDense) {
            extract_idx_masked(int_v[pos_base].data());
        } else {
            extract_idx_masked_dense(int_v[pos_base].data(), rtn_v.data());
        }

    } else if constexpr (std::is_same_v<T, UIntT>) {

        const size_t pos_base = find_col_base(matr_idx[4], 4);
        if constexpr (!IsDense) {
            extract_idx_masked(uint_v[pos_base].data());
        } else {
            extract_idx_masked_dense(uint_v[pos_base].data());
        }

    } else if constexpr (std::is_same_v<T, FloatT>) {

        const size_t pos_base = find_col_base(matr_idx[5], 5);
        if constexpr (!IsDense) {
            extract_idx_masked(dbl_v[pos_base].data());
        } else {
            extract_idx_masked_dense(dbl_v[pos_base].data(), rtn_v.data());
        }

    } else {
        std::cerr << "Error in (get_col), unsupported type\n";
        return;
    }
}




