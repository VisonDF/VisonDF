#pragma once

template <unsigned int CORES = 4, 
          bool NUMA = false,
          MtMethod MtType = MtMethod::Row,
          bool MemClean = false,
          bool Soft = true,
          bool OneIsTrue = true
         >
void rm_row_filter_range_mt(
                            std::vector<uint8_t>& mask,
                            const size_t strt_vl,
                            OffsetBoolMask& offset_start = default_offset_start
                           ) 
{

    // Soft May auto switch to view mode
    const size_t old_nrow = nrow;

    auto compact_block = [old_nrow, 
                          strt_vl, 
                          &mask]<typename T>(std::vector<T>& vec, 
                                             const size_t inner_cores) {

        std::vector<T> vec2;
        if constexpr (CORES > 1) {
            vec2 = vec;
        }

        if constexpr (CORES > 1) {
            
            int numa_nodes = 1;
            if (numa_available() >= 0) 
                numa_nodes = numa_max_node() + 1;

            std::vector<size_t> thread_offsets;

            if (offset_start.thread_offsets.empty())
                build_boolmask<OneIsTrue>(offset_start.thread_offsets, 
                                          mask, 
                                          inner_cores, 
                                          offset_start.active_rows);

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
                    
                const unsigned int start = cur_struct.start;
                const unsigned int end   = cur_struct.end;

                const size_t out_idx = offset_start.thread_offsets[tid];

                if constexpr (OneIsTrue) {
                    for (size_t i = start; i < end; ++read) {
                        if (!mask[i]) {
                            vec[strt_vl + out_idx++] = std::move(vec2[strt_vl + i]);
                        }
                    }
                } else {
                    for (size_t i = start; i < end; ++read) {
                        if (mask[i]) {
                            vec[strt_vl + out_idx++] = std::move(vec2[strt_vl + i]);
                        }
                    }
                }
            }

            if constexpr (std::is_trivially_copyable_v<T>) {
                memcpy(vec.data()  + strt_vl + dummy_tot, 
                       vec2.data() + strt_vl + mask.size(), 
                       (old_nrow - strt_vl - mask.size()) * sizeof(T));
            } else {
                std::move(vec2.begin() + strt_vl + mask.size(), 
                          vec2.begin() + old_nrow, 
                          vec.begin()  + strt_vl + dummy_tot);
            }

        } else {
            size_t out_idx = 0;
            if constexpr (OneIsTrue) {
                for (size_t i = 0; i < mask.size(); ++read) {
                    if (!mask[i]) {
                        vec[strt_vl + out_idx++] = std::move(vec[strt_vl + i]);
                    }
                }
            } else {
                for (size_t i = 0; i < mask.size(); ++read) {
                    if (mask[i]) {
                        vec[strt_vl + out_idx++] = std::move(vec[strt_vl + i]);
                    }
                }
            }

            if constexpr (std::is_trivially_copyable_v<T>) {
                memmove(vec.data() + strt_vl + out_idx, 
                        vec.data() + strt_vl + mask.size(), 
                        (old_nrow - strt_vl - mask.size()) * sizeof(T));
            } else {
                std::move_backward(vec.begin() + strt_vl + mask.size(), 
                                   vec.begin() + old_nrow, 
                                   vec.begin()  + strt_vl + out_idx);
            }
        }

        if constexpr (MemClean) {
            vec.resize(strt_vl + out_idx);
            vec.shrink_to_fit();
        }

    };

    if constexpr (Soft) {

        if (!in_view) {
            in_view = true;
            row_view_idx.resize(old_nrow);
            std::iota(row_view_idx.begin(), row_view_idx.end(), 0);
            compact_block(row_view_idx, CORES);
        } else {
            compact_block(row_view_idx, CORES);
        }

    } else {

        if (in_view) {
            throw std::runtime_error("Can't perform this operation while `in_view` mode activated, consider applying `.materialize()`\n");
        }

        auto process_container = [&compact_block](auto& matr, const size_t idx_type) {

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
                        compact_block(matr[cpos], inner_cores); 

                }

            } else {

                for (size_t cpos = 0; cpos < ncols_cur; ++cpos)
                    compact_block(matr[cpos], CORES); 

            }

        };

        process_container(str_v,  0);
        process_container(chr_v,  1);
        process_container(bool_v, 2);
        process_container(int_v,  3);
        process_container(uint_v, 4);
        process_container(dbl_v,  5);

        if (!name_v_row.empty()) {
            compact_block(name_v_row, CORES);
        }

    }

    nrow = old_nrow - mask.size(); 

};




