#pragma once

template <unsigned int CORES = 4,
          bool NUMA = false,
          bool MemClean = false,
          bool IsBool = false,
          bool MapCol = false,
          bool IsDense = false,
          typename T
        >
void get_col_filter_range(unsigned int x,
                          std::vector<T> &rtn_v,
                          const std::vector<uint8_t> &mask,
                          const unsigned int strt_vl)
{

    const unsigned int n_el = mask.size();
    rtn_v.reserve(n_el);

    auto find_col_base = [x](const auto &idx_vec, [[maybe_unused]] const size_t idx_type) -> size_t {
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

    auto extract_masked = [&rtn_v, &mask](const auto*__restrict src) {

        if constexpr (CORES > 1) {

            if (CORES > n_el)
                throw std::runtime_error("Too much cores for so little nrows\n");

            size_t active_count = 0;
            pre_active_rows.resize(n_el, 0);
            for (size_t i = 0; i < n_el; ++i) {
                active_count += mask[i] != 0;
                pre_active_rows[i] = active_count;
            }
            rtn_v.resize(active_count);

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
            
                size_t out_idx = pre_active_rows[start];

                for (size_t i = start; i < end; ++i) {
                    if (mask[i])
                        rtn_v[out_idx++] = src[strt_vl + i];
                }

            }

        } else {

            for (size_t i = 0; i < n_el; ++i) {
                if (mask[i])
                    rtn_v.push_back(src[strt_vl + i]);
            }

        }

    };

    auto extract_masked_dense = [&rtn_v, &mask]<typename TB>(const TB* __restrict src) {

        if constexpr (CORES > 1) {

            if (CORES > n_el)
                throw std::runtime_error("Too much cores for so little nrows\n");

            size_t active_count = 0;
            pre_active_rows.resize(n_el, 0);
            for (size_t i = 0; i < n_el; ++i) {
                active_count += mask[i] != 0;
                pre_active_rows[i] = active_count;
            }
            rtn_v.resize(active_count);

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
            
                size_t i       = start;
                size_t out_idx = pre_active_rows[start];

                while (i < end) {
                    while (i < end && mask[i] == 0) ++i;
                
                    const size_t cur_start = i;
                
                    while (i < end && mask[i] != 0) ++i;
                
                    const size_t len = i - cur_start;
                
                    memcpy(rtn_v.data() + out_idx,
                           src + cur_start,
                           len * sizeof(TB));
                    out_idx += len;
                }

            }

        } else {

            size_t active_count = 0;
            for (size_t i = 0; i < n_el; ++i) 
                active_count += mask[i] != 0;
            rtn_v.resize(active_count);

            size_t i       = 0;
            size_t out_idx = 0;
            
            while (i < n_el) {
                while (i < n_el && mask[i] == 0) ++i;
            
                const size_t cur_start = i;
            
                while (i < n_el && mask[i] != 0) ++i;
            
                const size_t len = i - cur_start;
            
                memcpy(rtn_v.data() + out_idx,
                       src + cur_start,
                       len * sizeof(TB));
                out_idx += len;
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

    if constexpr (MemClean && CORES > 1)
        rtn_v.shrink_to_fit();
}



