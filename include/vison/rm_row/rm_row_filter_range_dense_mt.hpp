#pragma once

template <unsigned int CORES = 4,
          bool NUMA = false,
          bool MemClean = false,
          bool Soft = true,
          bool OneIsTrue = true,
          AssertionType AssertionLevel = AssertionType::Simple
         >
void rm_row_range_dense_boolmask_mt(std::vector<uint8_t>& mask,
                                    const size_t strt_vl,
                                    OffsetBoolMask& start_offset)
{

    const size_t old_nrow = nrow;
    if (mask.empty() || old_nrow == 0) return;

    if (AssertionLevel > AssertionType::None) {

        const size_t new_nrow = (OneIsTrue) ? std::count(mask.begin(), mask.end(), 0) : std::count(mask.begin(), mask.end(), 1);
        if (new_nrow == nrow) return;
        if (new_nrow == 0) {
            std::cout << "Consider using .empty() if you want to remove all rows\n"; 
            return;
        }

    }

    auto compact_block_pod = [&mask]<typename T>(std::vector<T>& dst, 
                                                 const size_t inner_cores) {

        std::vector<T> src2;
        T* src;
        if constexpr (CORES > 1) {
            src2 = dst;
            src = &src2;
        } else {
            src = &dst;
        }

        if constexpr (CORES > 1) {

            int numa_nodes = 1;
            if (numa_available() >= 0) 
                numa_nodes = numa_max_node() + 1;

            std::vector<size_t> thread_counts;
            std::vector<size_t> thread_offsets;

            size_t dummy_tot;

            if (start_offset.vec.empty())
                boolmask_offset_per_thread<OneIsTrue>(thread_counts, 
                                                      thread_offsets, 
                                                      mask, 
                                                      inner_cores,
                                                      dummy_tot);

            #pragma omp parallel if(inner_cores > 1) num_threads(inner_cores)
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

                size_t out_idx;
                if (start_offset.vec.empty()) {
                    out_idx = thread_offsets[tid];
                } else {
                    out_idx = offset_start.vec[start];
                }

                size_t i = cur_start;
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
                while (i < end) {
      
                    if constexpr (OneIsTrue) {
                        while (i < end && mask[i]) {
                            i += 1;
                        }
                    } else {
                        while (i < end && !mask[i]) {
                            i += 1;
                        }
                    }

                    const size_t start = i;
                    if constexpr (OneIsTrue) {
                        while (i < end && !mask[i]) ++i;
                    } else {
                        while (i < end && mask[i]) ++i;
                    }
                
                    size_t len = i - start;
                    {
                        T* __restrict d = dst.data() + out_idx;
                        T* __restrict s = src + strt_vl + start;
       
                        memmove(d, s, len * sizeof(T))

                    }
                
                    out_idx += len;
                    i += 1;
                }

            }

            memcpy(vec.data() + strt_vl + dummy_tot, 
                   src + strt_vl + mask.size(), 
                   (old_nrow - mask.size() - strt_vl) * sizeof(T));

            if constexpr (MemClean) {
                dst.resize(strt_vl + dummy_tot);
                dst.shrink_to_fit();
            }

        } else {

            size_t i       = 0;
            size_t out_idx = strt_vl;
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

                size_t start = i;
                if constexpr (OneIsTrue) {
                    while (i < mask.size() && !mask[i]) ++i;
                } else {
                    while (i < mask.size() && mask[i]) ++i;
                }
            
                size_t len = i - start;
                {
                    T* __restrict d = dst.data() + out_idx;
                    T* __restrict s = src + strt_vl + start;
       
                    memmove(d, s, len * sizeof(T))

                }
            
                out_idx += len;
                i += 1;
            }

            memmove(vec.data() + strt_vl + out_idx, 
                    src + strt_vl + mask.size(), 
                    (old_nrow - mask.size() - strt_vl) * sizeof(T));

            if constexpr (MemClean) {
                dst.resize(out_idx + dummy_tot);
                dst.shrink_to_fit();
            }

        }

    };

    if constexpr (Soft) {

        if (!in_view) {
            row_view_idx.resize(old_nrow);
            std::iota(row_view_idx.begin(), row_view_idx.end(), 0);
            in_view = true;
        }

        compact_block_pod(row_view_idx, CORES);

    } else {

        if (in_view) {
            throw std::runtime_error("Can't perform this operation while `in_view` mode activated, consider applying `.materialize()`\n");
        }

        auto compact_block_scalar = [&mask](auto& vec, 
                                            const size_t inner_cores) {

            std::vector<std::string> vec2;
            if constexpr (CORES > 1) {
                vec2 = dst;
            }

            if constexpr (CORES > 1) {

                int numa_nodes = 1;
                if (numa_available() >= 0) 
                    numa_nodes = numa_max_node() + 1;

                std::vector<size_t> thread_counts;
                std::vector<size_t> thread_offsets;

                size_t dummy_tot;

                if (start_offset.vec.empty())
                    boolmask_offset_per_thread<OneIsTrue>(thread_counts, 
                                                          thread_offsets, 
                                                          mask, 
                                                          inner_cores, 
                                                          dummy_tot);

                #pragma omp parallel if(inner_cores > 1) num_threads(inner_cores)
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

                    size_t out_idx;
                    if (start_offset.vec.empty()) {
                        out_idx = thread_offsets[tid];
                    } else {
                        out_idx = offset_start.vec[start];
                    }

                    size_t i = cur_start;
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
                    while (i < end) {
      
                        if constexpr (OneIsTrue) {
                            while (i < end && mask[i]) i += 1;
                        } else {
                            while (i < end && !mask[i]) i += 1;
                        }

                        if constexpr (OneIsTrue) {
                           while (i < mask.size() && !mask[i]) {
                               vec[out_idx] = std::move(vec2[strt_vl + i]);
                               i += 1;
                               out_idx += 1;
                           };
                        } else {
                           while (i < mask.size() && mask[i]) {
                               vec[out_idx] = std::move(vec2[strt_vl + i]);
                               i += 1;
                               out_idx += 1;
                           };
                        }
                        i += 1;
                    }

                }

                std::move(vec.begin() + strt_vl + mask.size(), 
                          vec.begin() + old_nrow, 
                          vec2.begin() + strt_vl + dummy_tot);

            } else {

                size_t i       = 0;
                size_t out_idx = strt_vl;
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
                        while (i < mask.size() && mask[i]) i += 1;
                    } else {
                        while (i < mask.size() && mask[i]) i += 1;
                    }

                    if constexpr (OneIsTrue) {
                       while (i < mask.size() && !mask[i]) {
                           vec[written] = std::move(vec[strt_vl + i]);
                           i += 1;
                           out_idx += 1;
                       };
                    } else {
                       while (i < mask.size() && mask[i]) {
                           vec[written] = std::move(vec[strt_vl + i]);
                           i += 1;
                           out_idx += 1;
                       };
                    }

                    i += 1;
                }

                std::move_backward(vec.begin() + strt_vl + mask.size(), 
                                   vec.begin() + old_nrow, 
                                   vec.begin() + strt_vl + out_idx);

            }

        };

        auto process_container = [](auto&& f,
                                    auto& matr, 
                                    const size_t idx_type) 
        {

            const size_t ncols_cur = matr_idx[idx_type];

            if constexpr (CORES > 1) {

                int numa_nodes = 1;
                if (numa_available() >= 0) 
                    numa_nodes = numa_max_node() + 1;

                const unsigned int outer_cores = std::conditional_t<MtType == MtMethod::Col,
                                                                    CORES,
                                                                    std::min<unsigned int>(
                                                                        matr_idx[idx_type].size(), std::max(1u, CORES / 2)
                                                                     )
                                                                   >;
                const unsigned int inner_cores = std::conditional_t<MtType == MtMethod::Row,
                                                                    CORES,
                                                                    std::max(1u, CORES / outer_threads)
                                                                   >;

                #pragma omp parallel if(outer_cores > 1) num_threads(outer_cores)
                {

                    const int tid        = omp_get_thread_num();
                    const int nthreads   = omp_get_num_threads();
           
                    MtStruct cur_struct;

                    if constexpr (NUMA) {
                        numa_mt(cur_struct,
                                ncols_cur, 
                                tid, 
                                nthreads, 
                                numa_nodes);
                    } else {
                        simple_mt(cur_struct,
                                  ncols_cur, 
                                  tid, 
                                  nthreads);
                    }
                        
                    const unsigned int start = cur_struct.start;
                    const unsigned int end   = cur_struct.end;

                    for (size_t cpos = start; cpos < end; ++cpos)
                        f(matr[cpos], inner_cores); 

                }

            } else {

                for (size_t cpos = 0; cpos < ncols_cur; ++cpos)
                    f(matr[cpos], inner_cores); 

            }

        };

        process_container(compact_block_scalar, str_v,  0);
        process_container(compact_block_pod,    chr_v,  1);
        process_container(compact_block_pod,    bool_v, 2);
        process_container(compact_block_pod,    int_v,  3);
        process_container(compact_block_pod,    uint_v, 4);
        process_container(compact_block_pod,    dbl_v,  5);

        if (!name_v_row.empty()) {
            compact_block_scalar(name_v_row, CORES);
        }

    }

    nrow = new_nrow;

}


