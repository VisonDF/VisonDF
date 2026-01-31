#pragma once

template <unsigned int CORES           = 4,
          bool NUMA                    = false,
          bool IsBool                  = false,
          bool MapCol                  = false,
          AssertionType AssertionLevel = AssertionType::None,
          typename T> 
requires span_or_vec<T>
void rep_col_range_mt(
                      const T &x, 
                      const unsigned int colnb,
                      const unsigned int strt_vl, // start of the df col
                      const unsigned int start,
                      const unsigned int end
                     ) 
{

    static_assert(is_supported_type<element_type<T>>, "Error, type not supported\n");

    const unsigned int local_nrow = nrow;

    if constexpr (AssertionLevel > AssertionType::None) {
        if (!(end > start)) {
            std::runtime_error("end must be strictly superior to start\n");
        }
        if (end > local_nrow) {
            std::runtime_error("end out of bounds\n");
        }
    }

    auto find_col_base = [colnb]([[maybe_unused]] const auto &idx_vec, 
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

    const unsigned int len = end - start

    auto replace_pod = [len,
                        &find_col_index,
                        &x](
                            T* dst 
                           ) 
    {
    
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
                            len, 
                            tid, 
                            nthreads, 
                            numa_nodes);
                } else {
                    simple_mt(cur_struct,
                              len, 
                              tid, 
                              nthreads);
                }
                    
                const unsigned int cur_start = cur_struct.start;
                const unsigned int cur_len   = cur_struct.len;

                memcpy(dst + strt_vl, 
                       x.data() + start, 
                       cur_len * sizeof(T));

            }

        } else {

            memcpy(dst + strt_vl, 
                   x.data() + start, 
                   len * sizeof(T));

        }
    
    };

    auto replace_str = [&find_col_index,
                        &x](std::vector<std::string>& dst)
    {

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
                            x.size(), 
                            tid, 
                            nthreads, 
                            numa_nodes);
                } else {
                    simple_mt(cur_struct,
                              x.size(), 
                              tid, 
                              nthreads);
                }
                    
                const unsigned int start = cur_struct.start;
                const unsigned int len   = cur_struct.len;

                for (size_t i = start; i < start + len; ++i)
                    dst[strt_vl + i] = x[i];

            }

        } else {
            for (size_t i = 0; i < x.size(); ++i)
                dst[strt_vl + i] = x[i];
        }
    
    };

    if constexpr (IsBool) {

        const size_t pos_base = find_col_base(matr_idx[2], 2);
        replace_pod(bool_v[pos_base].data());

    } else if constexpr (std::is_same_v<element_type_t<T>, IntT>) {

        const size_t pos_base = find_col_base(matr_idx[3], 3);
        replace_pod(int_v[pos_base].data());

    } else if constexpr (std::is_same_v<element_type_t<T>, UIntT>) {

        const size_t pos_base = find_col_base(matr_idx[4], 4);
        replace_pod(uint_v[pos_base].data());

    } else if constexpr (std::is_same_v<element_type_t<T>, FloatT>) {

        const size_t pos_base = find_col_base(matr_idx[5], 5);
        replace_pod(dbl_v[pos_base].data());

    } else if constexpr (std::is_same_v<element_type_t<T>, std::string>) {

        const size_t pos_base = find_col_base(matr_idx[0], 0);
        replace_str(str_v[pos_base]);

    } else if constexpr (std::is_same_v<element_type_t<T>, CharT>) {

        const size_t pos_base = find_col_base(matr_idx[1], 1);
        replace_pod(chr_v[pos_base].data());

    } else {

        std::cerr << "Error unsupported type in (replace_col)\n";

    };

};




