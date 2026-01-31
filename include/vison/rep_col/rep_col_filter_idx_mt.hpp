#pragma once

template <unsigned int CORES           = 4,
          bool NUMA                    = false,
          bool IsBool                  = false,
          bool MapCol                  = false,
          bool IsDense                 = false,
          bool IdxIsTrue               = true,
          bool Periodic                = false,
          bool Move                    = false,  // side effects on mask, encouraged to be enabled is colnb corresponds to a string col
          AssertionType AssertionLevel = AssertionType::None,
          typename U,
          typename T> 
requires span_or_vec<T>
requires span_or_vec<U>
void rep_col_filter_idx_mt(
                           T& x, 
                           const unsigned int colnb,
                           const U& mask,
                           Runs& runs,
                           const unsigned int periodic_mask_len
                          )
{

    static_assert(std::is_same_v<
        typename std::remove_cvref_t<U>::value_type,
        unsigned int
    >, "Error, unsigned int for mask is required\n");

    static_assert(is_supported_type<element_type<T>>, "Error, type not supported\n");

    const unsigned int local_nrow = nrow;

    if constexpr (AssertionLevel == AssertionType::Hard) {
        if constexpr (!IdxIsTrue || IsDense) {
            unsigned int ref_val = mask[0];
            for (size_t i = 1; i < mask.size(); ++i) {
                if (ref_val < mask[i]) 
                    throw std::runtime_error("mask is not sorted increasingly\n");
                ref_val = mask[i];
            }
        } else {
            for (auto el : mask) {
                if (el >= local_nrow)
                    throw std::runtime_error("mask index out of bouds\n");
            }
        }
    }

    if constexpr (AssertionLevel > AssertionType::None) {
        if constexpr (!IdxIsTrue) {
            if (mask.back() >= local_nrow)
                throw std::runtime_error("mask indices are exceeding nrow\n");
        }
    }

    if constexpr (AssertionLevel > AssertionType::None) {
        if (mask.size() >= local_nrow)
            throw std::runtime_error("mask out of bound\n");
    }

    const unsigned int n_el  = (!Periodic) ? mask.size() : periodic_mask_len;
    const unsigned int n_el2 = mask.size();

    auto find_col_base = [this,
                          x]([[maybe_unused]] const auto &idx_vec, 
                             [[maybe_unused]] const size_t idx_type) -> size_t 
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

    auto replace_pod = [&mask,
                        &runs,
                        &x,
                        n_el,
                        n_el2](std::string* dst) 
    {

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
                const size_t out_idx         = offset_start.thread_offsets[tid];

                copy_col_filter_idx<IdxIsTrue,
                                    Periodic,
                                    Move      // copy
                                   >(
                                     dst,
                                     x.data(),
                                     mask,
                                     out_idx,
                                     cur_start,
                                     cur_end,
                                     n_el2
                                    );

        } else {

            size_t out_idx = 0; // dummy val

            copy_col_filter_idx<IdxIsTrue,
                                Periodic,
                                Move      // copy
                               >(
                                  dst,
                                  x.data(),
                                  mask,
                                  out_idx,
                                  0,
                                  n_el,
                                  n_el2
                                 );

        }
    };

    auto replace_pod_dense = [&mask,
                              &runs,
                              &x,
                              n_el,
                              n_el2]<typename T>(T* dst) 
    {

        if constexpr (CORES > 1) {

            std::vector<size_t> thread_counts;
            if (runs.thread_offsets.empty()) {
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

                const size_t delta  = runs.thread_counts[tid];
                const size_t delta2 = (!IdxIsTrue) ? runs.thread_offsets[tid] : 0;   // else dummy val

                copy_col_filter_idx_dense<IdxIsTrue,
                                          Periodic,
                                          true      // copy (distinct)
                                         >(
                                            dst,
                                            x.data(),
                                            runs,
                                            delta,
                                            delta2,
                                            cur_start,
                                            cur_end
                                          );

        } else {

            if (runs.thread_offsets.empty()) {
                build_runs<IdxIsTrue,
                           Periodic>(
                                     mask,
                                     runs.vec,
                                     runs.active_rows,
                                     n_el,
                                     n_el2
                                    );
            }

            const size_t delta  = 0;  // dummy val
            const size_t delta2 = 0;  // dummy val

            copy_col_filter_idx_dense<IdxIsTrue,
                                         Periodic,
                                         true      // copy (distinct)
                                        >(
                                           dst,
                                           x.data(),
                                           runs,
                                           delta,
                                           delta2,
                                           0,
                                           n_el
                                         );

        }
    };


    if constexpr (IsBool) {

        const size_t pos_base = find_col_base(matr_idx[2], 2);

        if constexpr (IsDense) {

            replace_pod_dense(bool_v[pos_base].data());

        } else {

            replace_pod(bool_v[pos_base].data());

        }

    } else if constexpr (std::is_same_v<T, IntT>) {

        const size_t pos_base = find_col_base(matr_idx[3], 3);

        if constexpr (IsDense) {

            replace_numeric_dense(int_v[pos_base].data());

        } else {

            replace_numeric(int_v[pos_base].data());

        }

    } else if constexpr (std::is_same_v<T, UIntT>) {

        const size_t pos_base = find_col_base(matr_idx[4], 4);

        if constexpr (IsDense) {

            replace_numeric_dense(uint_v[pos_base].data());

        } else {

            replace_numeric(uint_v[pos_base].data());

        }

    } else if constexpr (std::is_same_v<T, FloatT>) {

        const size_t pos_base = find_col_base(matr_idx[5], 5);

        if constexpr (IsDense) {

            replace_numeric_dense(dbl_v[pos_base].data());

        } else {

            replace_numeric(dbl_v[pos_base].data());

        }

    } else if constexpr (std::is_same_v<T, std::string>) {

        const size_t pos_base = find_col_base(matr_idx[0], 0);
        replace_pod(str_v[pos_base].data());

    } else if constexpr (std::is_same_v<T, CharT>) {
        
        const size_t pos_base = find_col_base(matr_idx[1], 1);

        if constexpr (IsDense) {

            replace_pod_dense(chr_v[pos_base].data());

        } else {

            replace_pod(chr_v[pos_base].data());

        }


    } else {
        std::cerr << "Error unsupported type in (replace_col)\n";
    }
}

template <unsigned int CORES           = 4,
          bool NUMA                    = false,
          bool IsBool                  = false,
          bool MapCol                  = false,
          bool IsDense                 = false,
          bool IdxIsTrue               = true,
          bool Periodic                = false,
          bool Move                    = false,  // side effects on mask, encouraged to be enabled is colnb corresponds to a string col
          AssertionType AssertionLevel = AssertionType::None,
          typename U,
          typename T> 
requires span_or_vec<T>
requires span_or_vec<U>
void rep_col_filter_idx_mt(
                           T& x, 
                           const unsigned int colnb,
                           const U& mask,
                           Runs& runs = default_idx_runs,
                          )
{

    rep_col_filter_idx_mt<CORES,
                          NUMA,
                          IsBool,
                          MapCol,
                          IsDense,
                          IdxIsTrue,
                          Periodic,
                          Move>rep_col_filter_idx_mt(
        x,
        colnb,
        mask,
        runs,
        nrow
    );

}




