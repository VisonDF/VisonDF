#pragma once

template <bool MapCol = false,
          unsigned int CORES = 4,
          bool OneIsTrue = true,
          typename T, 
          typename F>
inline void apply_numeric_filter_boolmask(const std::vector<T>& values, 
                                          const unsigned int n, 
                                          const size_t idx_type, 
                                          F&& f,
                                          const std::vector<uint8_t>& mask,
                                          const unsigned int strt_vl,
                                          OffsetBoolMask& offset_start) 
{
    unsigned int i2 = 0;
    if constexpr (!MapCol) {
        i2 = 0;
        while (i2 < matr_idx[idx_type].size()) {
            if (n == matr_idx[idx_type][i2])
                break;
            ++i2;
        }
    } else {
        if (!matr_idx_map[idx_type].contains(n)) {
            std::cerr << "MapCol used but col not found in the map\n";
            return;
        }
        if (!sync_map_col[idx_type]) {
            std::cerr << "Col not synced\n";
            return;
        }

        i2 = matr_idx_map[idx_type][n];

    }

    std::vector<T>& dst = values[i2];
    const unsigned int end_val = mask.size();

    if constexpr (CORES > 1) {

        if (CORES > mask.size())
            throw std::runtime_error("Too much cores for so little nrows\n");

       std::vector<size_t> thread_counts;
       std::vector<size_t> thread_offsets;

        size_t active_count;
        if (offset_start.vec.empty()) {
            boolmask_offset_per_thread<OneIsTrue>(thread_counts, 
                                                  thread_offsets, 
                                                  mask, 
                                                  CORES,
                                                  active_count);
        }

        int numa_nodes = 1;
        if (numa_available() >= 0) 
            numa_nodes = numa_max_node() + 1;

        #pragma omp prallel num_threads(CORES)
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
            if (offset_start.vec.empty()) {
                out_idx = thread_offsets[tid];
            } else {
                out_idx = offset_start[cur_start];
            }

            if constexpr (OneIsTrue) {

                for (size_t i = cur_start; i < cur_end; ++i) {
                    if (!mask[i]) { continue; }
                    f(dst[strt_vl + out_idx]);
                    out_idx += 1;
                }

            } else {

                for (size_t i = cur_start; i < cur_end; ++i) {
                    if (mask[i]) { continue; }
                    f(dst[strt_vl + out_idx]);
                    out_idx += 1;
                }

            }

        }

    } else {

        size_t out_idx = 0;

        if constexpr (OneIsTrue) {

            for (size_t i = 0; i < end_val; ++i) {
                if (!mask[i]) { continue; }
                f(dst[strt_vl + out_idx]);
                out_idx += 1;
            }

        } else {

            for (size_t i = 0; i < end_val; ++i) {
                if (mask[i]) { continue; }
                f(dst[strt_vl + out_idx]);
                out_idx += 1;
            }

        }

    }
}







