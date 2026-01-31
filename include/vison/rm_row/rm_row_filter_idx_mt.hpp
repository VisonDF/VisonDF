#pragma once

template <unsigned int CORES           = 4, 
          bool NUMA                    = false,
          MtMethod MtType              = MtMethod::Row,
          bool MemClean                = false,
          bool Soft                    = true,
          bool IdxIsTrue               = true,
          bool Periodic                = false,
          AssertionType AssertionLevel = AssertionType::Simple,
          typename T
         >
requires span_or_vec<T>
void rm_row_filter_idx_mt(
                          const T& mask,
                          Runs& runs,
                          const unsigned int periodic_mask_len
                         ) 
{

    static_assert(std::is_same_v<
        typename std::remove_cvref_t<T>::value_type,
        unsigned int
    >, "Error, unsigned int for mask is required\n");

    // Soft May auto switch to view mode
    const size_t old_nrow = nrow;

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

    const size_t n_el  = (Periodic) ? periodic_mask_len : mask.size();
    const size_t n_el2 = mask.size();

    if constexpr (CORES > 1) {
        if (runs.thread_offsets.empty()) {
            build_runs_mt_simple<IdxIsTrue,
                                 Periodic>(
                                           runs.thread_offsets,
                                           mask,
                                           CORES,
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

    auto compact_block = [n_el,
                          n_el2,
                          &runs,
                          &mask]<typename T>(std::vector<T>& vec, 
                                             const size_t inner_cores) {

        T* dst = vec.data();
        T* src;
        std::vector<T> vec2;
        if constexpr (CORES > 1) {
            vec2 = vec;
            src = &vec2;
        } else {
            src = &vec;
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

                size_t out_idx = (!IdxIsTrue) ? runs.thread_offsets[tid] : 0; // else dummy_val
                copy_col_filter_idx<IdxIsTrue,
                                    Periodic,
                                    false      // distinct but wants move
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
};

template <unsigned int CORES           = 4, 
          bool NUMA                    = false,
          MtMethod MtType              = MtMethod::Row,
          bool MemClean                = false,
          bool Soft                    = true,
          bool IdxIsTrue               = true,
          bool Periodic                = false,
          AssertionType AssertionLevel = AssertionType::Simple,
          typename T
         >
requires span_or_vec<T>
void rm_row_filter_idx_mt(
                          const T& mask,
                          Runs& runs = default_idx_runs,
                         ) 
{

    rm_row_filter_idx_mt<CORES,
                         NUMA,
                         MtType,
                         MemClean,
                         Soft,
                         IdxIsTrue,
                         Periodic,
                         AssertionLevel>rm_row_filter_idx_mt(
        mask,
        runs,
        nrow
    );

}




