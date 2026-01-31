#pragma once

template <unsigned int CORES           = 4,
          bool NUMA                    = false,
          bool IsBool                  = false,
          bool MapCol                  = false,
          bool IsDense                 = false,
          bool OneIsTrue               = true,
          bool Periodic                = false,
          bool Move                    = false,  // side effects on mask, encouraged to be enabled is colnb corresponds to a string col
          AssertionType AssertionLevel = AssertionType::None,
          typename T,
          typename U> 
requires vector_or_span<U>
requires vector_or_span<T>
void rep_col_filter_range_mt(
                               T& x, 
                               const unsigned int colnb,
                               const U& mask,
                               const unsigned int strt_vl,
                               OffsetBoolMask& offset_start,
                               const unsigned int periodic_mask_len
                             )
{

    static_assert(std::is_same_v<
        typename std::remove_cvref_t<U>::value_type,
        uint8_t
    >, "Error, uint8_t for mask is required\n");

    static_assert(is_supported_type<element_type<T>>, "Error, type not supported\n");

    const unsigned int local_nrow = nrow;

    if constexpr (AssertionLevel > AssertionType::None) {
        if (x.size() != mask.size()) {
            throw std::runtime_error("vector and mask have different size\n");
        }
        if (strt_vl + x.size() != local_nrow) {
            throw std::runtime_error("Vector out of bound\n");
        }
        if (!(strt_vl + periodic_mask_len < local_nrow)) {
            throw std::runtime_error("!(strt_vl + periodic_mask_len < local_nrow)\n");
        }
    }
 
    auto find_col_base = [this, 
                          colnb]([[maybe_unused]] const auto &idx_vec, 
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

    const unsigned int n_el  = (!Periodic) ? mask.size() : periodic_mask_len;
    const unsigned int n_el2 = mask.size();

    auto replace_pod = [&mask,
                        &x,
                        &offset_start,
                        n_el,
                        n_el2](
                               T* dst 
                              ) 
    {
  
        if constexpr (CORES > 1) {

            if (offset_start.vec.empty()) {
                build_boolmask<OneIsTrue,
                               Periodic>(offset_start.thread_offsets, 
                                         mask, 
                                         CORES,
                                         offset_start.active_rows,
                                         n_el2);
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
                const size_t out_idx         = offset_start.thread_offsets[tid];

                copy_col_filter_range<OneIsTrue,
                                      Periodic,
                                      Move       
                                      >(
                                        dst,
                                        x.data(),
                                        mask,
                                        out_idx,
                                        cur_start,
                                        cur_end,
                                        n_el2
                                       );

            }

        } else {

            size_t out_idx = 0;  // dummy_val

            copy_col_filter_range<OneIsTrue,
                                  Periodic,
                                  Move 
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
                              &x,
                              &offset_start,
                              n_el,
                              n_el2](
                                     T* dst 
                                    ) 
    {
  
        if (offset_start.vec.empty()) {
            build_boolmask<OneIsTrue,
                           Periodic>(offset_start.thread_offsets, 
                                     mask, 
                                     CORES,
                                     offset_start.active_rows,
                                     n_el2);
        }

        if constexpr (CORES > 1) {

            if (CORES > x.size())
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

                copy_col_filter_range_dense<
                                            OneIsTrue,
                                            Periodic,
                                            true     // distinct
                                           >(
                                              dst,
                                              x.data(),
                                              mask,
                                              cur_start,
                                              out_idx,
                                              cur_start,
                                              cur_end,
                                              n_el2
                                            );

            }

        } else {

            copy_col_filter_range_dense<
                                        OneIsTrue,
                                        Periodic,
                                        true     // distinct
                                       >(
                                          dst,
                                          x.data(),
                                          mask,
                                          0,
                                          0,
                                          0,
                                          n_el,
                                          n_el2
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

    } else if constexpr (std::is_same_v<element_type_t<T>, IntT>) {

        const size_t pos_base = find_col_base(matr_idx[3], 3);

        if constexpr (IsDense) {

            replace_pod_dense(int_v[pos_base].data());

        } else {

            replace_pod(int_v[pos_base].data());

        }

    } else if constexpr (std::is_same_v<element_type_t<T>, UIntT>) {

        const size_t pos_base = find_col_base(matr_idx[4], 4);

        if constexpr (IsDense) {

            replace_pod_dense(uint_v[pos_base].data());

        } else {

            replace_pod(uint_v[pos_base].data());

        }

    } else if constexpr (std::is_same_v<element_type_t<T>, FloatT>) {

        const size_t pos_base = find_col_base(matr_idx[5], 5);

        if constexpr (IsDense) {

            replace_pod_dense(dbl_v[pos_base].data());

        } else {

            replace_pod(dbl_v[pos_base].data());

        }

    } else if constexpr (std::is_same_v<element_type_t<T>, std::string>) {

        const size_t pos_base = find_col_base(matr_idx[0], 0);

        replace_str(str_v[pos_base]);

    } else if constexpr (std::is_same_v<element_type_t<T>, CharT>) {

        const size_t pos_base = find_col_base(matr_idx[1], 1);

        if constexpr (IsDense) {

            replace_pod_dense(chr_v[pos_base].data());

        } else {

            replace_pod(chr_v[pos_base].data());

        }

    } else {
        std::cerr << "Error unsupported type in (replace_col)\n";
    };
}

template <unsigned int CORES           = 4,
          bool NUMA                    = false,
          bool IsBool                  = false,
          bool MapCol                  = false,
          bool IsDense                 = false,
          bool OneIsTrue               = true,
          bool Periodic                = false,
          bool Move                    = false,
          AssertionType AssertionLevel = AssertionType::None,
          typename T,
          typename U>
requires vector_or_span<U>
requires vector_or_span<T>
void rep_col_filter_range_mt(
    T& x,
    unsigned int colnb,
    const U& mask,
    unsigned int strt_vl,
    OffsetBoolMask& offset_start = default_offset_start
) 
{
    rep_col_filter_range_mt<CORES,
                            NUMA,
                            IsBool,
                            MapCol,
                            IsDense,
                            OneIsTrue,
                            Periodic,
                            Move,
                            AssertionLevel>(
        x,
        colnb,
        mask,
        strt_vl,
        offset_start,
        (nrow - strt_vl)
    );
}




