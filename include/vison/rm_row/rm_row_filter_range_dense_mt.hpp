#pragma once

template <unsigned int CORES           = 4,
          bool NUMA                    = false,
          bool MemClean                = false,
          bool Soft                    = true,
          bool OneIsTrue               = true,
          bool Periodic                = false,
          AssertionType AssertionLevel = AssertionType::Simple
         >
void rm_row_range_dense_boolmask_mt(std::vector<uint8_t>& mask,
                                    const size_t strt_vl,
                                    OffsetBoolMask& offset_start)
{

    const size_t old_nrow = nrow;
    if (mask.empty() || old_nrow == 0) return;

    if constexpr (AssertionLevel > AssertionType::None) {
        if (strt_vl + mask.size() >= old_nrow)
            throw std::runtime_error("strt_vl + mask.size() out of bounds\n");
    }

    if constexpr (AssertionLevel > AssertionType::Simple) {

        const size_t new_nrow = (OneIsTrue) ? std::count(mask.begin(), mask.end(), 0) : std::count(mask.begin(), mask.end(), 1);
        if (new_nrow == nrow) return;
        if (new_nrow == 0) {
            std::cout << "Consider using .empty() if you want to remove all rows\n"; 
            return;
        }

    }

    const unsigned int n_el  = (Periodic) ? old_nrow - strt_vl : mask.size();
    const unsigned int n_el2 = mask.size();

    if (offset_start.thread_offsets.empty())
        build_boolmask<OneIsTrue,
                       Periodic>(offset_start.thread_offsets, 
                                 mask, 
                                 inner_cores, 
                                 offset_start.active_rows,
                                 n_el2);

    auto compact_block_pod = [n_el,
                              n_el2,
                              strt_vl,
                              &offset_start,
                              &mask]<typename T>(std::vector<T>& dst_vec, 
                                                 const size_t inner_cores) {

        T* dst = dst_vec.data() + strt_vl;
        std::vector<T> src2;
        T* src;
        if constexpr (CORES > 1) {
            src2 = dst;
            src = src2.data() + strt_vl;
        } else {
            src = dst.data() + strt_vl;
        }

        if constexpr (CORES > 1) {

            int numa_nodes = 1;
            if (numa_available() >= 0) 
                numa_nodes = numa_max_node() + 1;

            #pragma omp parallel if(inner_cores > 1) num_threads(inner_cores)
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

                copy_col_dense<
                               OneIsTrue,
                               Periodic,
                               true     // distinct
                              >c(
                                  dst,
                                  src,
                                  mask,
                                  i,
                                  out_idx,
                                  cur_start,
                                  cur_end,
                                  n_el2
                                );
            }

            if constexpr (!Periodic) {
                memcpy(dst + offset_start.active_rows, 
                       src + mask.size(), 
                       (old_nrow - mask.size() - strt_vl) * sizeof(T));
            }

        } else {

            size_t i       = 0;
            size_t out_idx = 0;

            copy_col_dense<
                           OneIsTrue,
                           Periodic,
                           false     // not distinct
                          >(
                             dst,
                             src,
                             mask,
                             i,
                             out_idx,
                             0,
                             n_el,
                             n_el2
                           );

            if constexpr (!Periodic) {
                memmove(dst + offset_start.active_rows, 
                        src + mask.size(), 
                        (old_nrow - mask.size() - strt_vl) * sizeof(T));
            }

        }

        if constexpr (MemClean) {
            dst.resize(strt_vl + offset_start.active_rows + (old_nrow - strt_vl - n_el));
            dst.shrink_to_fit();
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

        auto compact_block_scalar = [strt_vl,
                                     n_el,
                                     n_el2,
                                     &offset_start,
                                     &mask](std::vector<std::string>& dst_vec, 
                                            const size_t inner_cores) {

            std::string* dst = dst_vec.data() + strt_vl;
            std::string* src;
            std::vector<std::string> dst_vec2;
            if constexpr (CORES > 1) {
                dst_vec2 = dst_vec;
                src = dst_vec2.data() + strt_vl;
            } else {
                src = dst_vec.data() + strt_vl;
            }

            if constexpr (CORES > 1) {

                int numa_nodes = 1;
                if (numa_available() >= 0) 
                    numa_nodes = numa_max_node() + 1;

                #pragma omp parallel if(inner_cores > 1) num_threads(inner_cores)
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
                        while (i < end) {
      
                            if constexpr (OneIsTrue) {
                                while (i < cur_end && mask[i]) i += 1;
                            } else {
                                while (i < cur_end && !mask[i]) i += 1;
                            }

                            if constexpr (OneIsTrue) {
                               while (i < cur_end && !mask[i]) {
                                   vec[out_idx] = std::move(vec2[i]);
                                   i += 1;
                                   out_idx += 1;
                               };
                            } else {
                               while (i < cur_end && mask[i]) {
                                   vec[out_idx] = std::move(vec2[i]);
                                   i += 1;
                                   out_idx += 1;
                               };
                            }
                            i += 1;
                        }
                    } else {

                        size_t k = cur_start % n_el2;

                        if constexpr (OneIsTrue) {
                            while (!mask[k]) {
                                i += 1;
                                k += 1;
                                out_idx += 1;
                                k -= (k == n_el2) * n_el2;
                            }
                        } else {
                            while (mask[k]) {
                                i += 1;
                                k += 1;
                                out_idx += 1;
                                k -= (k == n_el2) * n_el2;
                            }
                        }
                        while (i < end) {
      
                            if constexpr (OneIsTrue) {
                                while (i < cur_end && mask[k]) {
                                    i += 1;
                                    k += 1;
                                    k -= (k == n_el2) * n_el2;
                                }
                            } else {
                                while (i < cur_end && !mask[k]) {
                                    i += 1;
                                    k += 1;
                                    k -= (k == n_el2) * n_el2;
                                }
                            }

                            if constexpr (OneIsTrue) {
                               while (i < cur_end && !mask[k]) {
                                   vec[out_idx] = std::move(vec2[i]);
                                   i += 1;
                                   k += 1;
                                   out_idx += 1;
                                   k -= (k == n_el2) * n_el2;
                               };
                            } else {
                               while (i < cur_end && mask[k]) {
                                   vec[out_idx] = std::move(vec2[i]);
                                   i += 1;
                                   k += 1;
                                   out_idx += 1;
                                   k -= (k == n_el2) * n_el2;
                               };
                            }
                            i += 1;
                        }
                    }
                }

                if constexpr (!Periodic) {
                    std::move(src + mask.size(), 
                              src + (old_nrow - strt_vl), 
                              dst + offset_start.active_rows);
                }

            } else {

                size_t i       = 0;
                size_t out_idx = 0;
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
                            while (i < mask.size() && mask[i]) i += 1;
                        } else {
                            while (i < !mask.size() && mask[i]) i += 1;
                        }

                        if constexpr (OneIsTrue) {
                           while (i < mask.size() && !mask[i]) {
                               vec[out_idx] = std::move(vec[i]);
                               i += 1;
                               out_idx += 1;
                           };
                        } else {
                           while (i < mask.size() && mask[i]) {
                               vec[out_idx] = std::move(vec[i]);
                               i += 1;
                               out_idx += 1;
                           };
                        }

                        i += 1;
                    }
                } else {

                    size_t k = 0;

                    if constexpr (OneIsTrue) {
                        while (!mask[k]) {
                            i += 1;
                            k += 1;
                            out_idx += 1;
                            k -= (k == n_el2) * n_el2;
                        }
                    } else {
                        while (mask[k]) {
                            i += 1;
                            k += 1;
                            out_idx += 1;
                            k -= (k == n_el2) * n_el2;
                        }
                    }
                    while (i < mask.size()) {
                        if constexpr (OneIsTrue) {
                            while (i < mask.size() && mask[k]) {
                                i += 1;
                                k += 1;
                                k -= (k == n_el2) * n_el2;
                            }
                        } else {
                            while (i < mask.size() && !mask[k]) {
                                i += 1;
                                k += 1;
                                k -= (k == n_el2) * n_el2;
                            }
                        }

                        if constexpr (OneIsTrue) {
                           while (i < mask.size() && !mask[k]) {
                               vec[out_idx] = std::move(vec[i]);
                               i += 1;
                               k += 1;
                               out_idx += 1;
                               k -= (k == n_el2) * n_el2;
                           };
                        } else {
                           while (i < mask.size() && mask[k]) {
                               vec[out_idx] = std::move(vec[i]);
                               i += 1;
                               k += 1;
                               out_idx += 1;
                               k -= (k == n_el2) * n_el2;
                           };
                        }

                        i += 1;
                    }
                }

                if constexpr (!Periodic) {
                    std::move_backward(src + mask.size(), 
                                       src + (old_nrow - strt_vl), 
                                       dst + out_idx);
                }
            }

            if constexpr (MemClean) {
                dst.resize(strt_vl + offset_start.active_rows + (old_nrow - strt_vl - n_el));
                dst.shrink_to_fit();
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


