#pragma once

template <unsigned int CORES           = 4,
          bool NUMA                    = false,
          MtMethod MtType              = MtMethod::Row,
          bool MemClean                = false,
          bool Soft                    = true,
          bool IdxIsTrue               = true,
          AssertionType AssertionLevel = AssertionType::Normal
         >
void rm_row_filter_idx_dense_mt(std::vector<unsigned int>& mask,
                                Runs& runs = Runs{})
{

    const size_t old_nrow = nrow;
    if (x.empty() || old_nrow == 0) return;

    const size_t n_el = (IdxIsTrue) ? n_el : old_nrow - n_el;

    if constexpr (AssertionLevel == AssertionType::Hard) {
        if constexpr (!IdxIsTrue) {
            unsigned int ref_val = mask[0];
            for (size_t i = 1; i < mask.size(); ++i) {
                if (ref_val < mask[i]) 
                    throw std::runtime_error("mask is not sorted increasingly\n");
                ref_val = mask[i];
            }
        } else {
            for (auto el : mask) {
                if (el >= old_nrow)
                    throw std::runtime_error("mask index out of bouds\n");
            }
        }
    }

    if constexpr (AssertionLevel > AssertionType::None) {
        if constexpr (!IdxIsTrue) {
            if (mask.back() >= old_nrow)
                throw std::runtime_error("mask indices are exceeding nrow\n");
        }
    }

    const size_t n_el  = (Periodic) ? old_nrow : mask.size();
    const size_t n_el2 = mask.size();
    if constexpr (CORES > 1) {
        if (runs.vec.empty() || runs.thread_offsets.empty()) {
            build_runs_mt<IdxIsTrue,
                          Periodic>(
                                    runs.thread_offsets,
                                    thread_counts,
                                    mask,
                                    CORES,
                                    runs,
                                    runs.active_rows,
                                    n_el,
                                    n_el2
                                   );
        }
    } else {
        if (runs.thread_offsets.empty()) {
            build_runs_simple<IdxIsTrue,
                              Periodic>(
                                        mask,
                                        active_rows,
                                        n_el,
                                        n_el2
                                       );
        }
    }
    nrow = runs.active_rows;

    auto compact_block_pod = [n_el,
                              n_el2,
                              &runs,
                              &mask]<typename T>(std::vector<T>& dst,
                                                 std::vector<T>& src,
                                                 const size_t inner_threads)
    {

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

                const size_t delta  = runs.thread_counts[tid];
                const size_t delta2 = (!IdxIsTrue) ? runs.thread_offsets[tid] : 0; // else dummy val

                copy_col_filter_idx_dense<IdxIsTrue,
                                          Periodic,
                                          false      // copy (distinct)
                                         >(
                                            dst.data(),
                                            src.data(),
                                            runs,
                                            delta,
                                            delta2,
                                            cur_start,
                                            cur_end
                                          );

            }

        } else {

            const size_t delta  = 0;  // dummy val
            const size_t delta2 = 0;  // dummy val

            copy_col_filter_idx_dense<IdxIsTrue,
                                         Periodic,
                                         false      // copy (distinct)
                                        >(
                                           dst.data(),
                                           src.data(),
                                           runs,
                                           delta,
                                           delta2,
                                           0,
                                           n_el
                                         );

        }

        if constexpr (MemClean) {
            vec.resize(n_el);
            vec.shrink_to_fit();
        }
    };

    if constexpr (Soft) {

        if constexpr (AssertionLevel == AssertionType::Hard) {
        }

        if (!in_view) {
            in_view = true;
            row_view_idx.resize(new_nrow);
            std::iota(row_view_idx.begin(), row_view_idx.end(), 0);
        }

        compact_block_pod(row_view_idx, row_view_idx, CORES);

    } else {

        if (in_view) {
            std::cerr << "Can't perform this operation while `in_view` mode activated, consider applying `.materialize()`\n";
            return;
        }

        auto compact_block_scalar = [n_el,
                                     n_el2,
                                     &runs,
                                     &mask]<typename T>(std::vector<T>& vec, 
                                                        const size_t inner_cores) {

            T* dst = vec.data();
            T* src = vec.data();

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

                    size_t out_idx = (!IdxIsTrue) ? runs.thread_offsets[tid] : 0; // else dummy_val
                    copy_col_filter_idx<IdxIsTrue,
                                        Periodic,
                                        false      // move
                                       >(
                                         dst,
                                         src,
                                         mask,
                                         out_idx,
                                         cur_start,
                                         cur_end,
                                         n_el2
                                        );

                }

            } else {

                size_t out_idx = 0; // dummy val
                copy_col_filter_idx<IdxIsTrue,
                                    Periodic,
                                    false      // move
                                   >(
                                      dst,
                                      src,
                                      mask,
                                      out_idx,
                                      0,
                                      n_el,
                                      n_el2
                                     );
            }

            if constexpr (MemClean) {
                vec.resize(n_el);
                vec.shrink_to_fit();
            }

        };

        auto process_container = [&mask](auto&& f,
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
}





