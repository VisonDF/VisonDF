#pragma once

struct RunMt {
    size_t mask_pos;
    size_t src_start;
    size_t len;
};

template <unsigned int CORES = 4,
          bool NUMA = false,
          bool IsBool = false,
          bool MapCol = false,
          bool IsDense = false,
          typename T>
void get_col_filter_idx_mt(unsigned int x,
                           std::vector<T> &rtn_v,
                           const std::vector<unsigned int> &mask)
{
    rtn_v.resize(mask.size());

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

    auto extract_idx_masked = [&rtn_v, &mask](const auto*__restrict src) {
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
                    
                const unsigned int start = cur_struct.start;
                const unsigned int end   = cur_struct.end;

                for (size_t i = start; i < end; ++i)
                    rtn_v[i] = src[mask[i]];

            }

        } else {
            for (size_t i = 0; i < mask.size(); ++i)
                rtn_v[i] = src[mask[i]];
        }
    };

    auto extract_idx_masked_dense = [&mask]<typename T>(
                                      T* __restrict dst,
                                      const T* __restrict src,
                                      ) {
 
        if constexpr (CORES > 1) {

            std::vector<RunMt> runs;
            
            for (size_t i = 0; i < mask.size();) {
                size_t start = i;
                size_t src_start = mask[i];
            
                while (i + 1 < mask.size() &&
                       mask[i + 1] == mask[i] + 1) {
                    ++i;
                }
            
                runs.push_back({start, src_start, i - start + 1});
                ++i;
            }

            int numa_nodes = 1;
            if (numa_available() >= 0) 
                numa_nodes = numa_max_node() + 1;

            #pragma omp parallel num_threads(CORES)
            {
                const int tid        = omp_get_thread_num();
                const int nthreads   = omp_get_num_threads();
           
                size_t start;
                size_t end;
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
                    
                start = cur_struct.start;
                end   = cur_struct.end;

                for (size_t r = start; r < end; ++r) {
                    const auto& run = runs[r]; 
                    std::memcpy(dst.data() + run.mask_pos,
                                src.data() + run.src_start,
                                run.len * sizeof(T));
                }
            }

        } else {
            size_t pos = 0;
            size_t dst_offset = 0;
            while (pos < mask.size()) {
                size_t run_start = pos;
                size_t src_start = mask[pos];
    
                while (pos + 1 < mask.size() &&
                       mask[pos + 1] == mask[pos] + 1) {
                    ++pos;
                }
    
                size_t run_len = pos - run_start + 1;
    
                std::memcpy(dst.data() + dst_offset,
                            src.data() + src_start,
                            run_len * sizeof(T));
                
                dst_offset += run_len;
                ++pos;
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




