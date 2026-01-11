#pragma once

template <bool OneIsTrue>
inline void boolmask_offset_per_thread(std::vector<size_t>& thread_counts, 
                                       std::vector<size_t>& thread_offsets,
                                       const std::vector<uint8_t>& mask,
                                       const size_t inner_cores,
                                       size_t& active_rows)
{
    thread_counts.resize(inner_cores);
    thread_offsets.resize(inner_cores);

    #pragma omp parallel if(inner_cores > 1) num_threads(inner_cores)
    {
        const int tid = omp_get_thread_num();
        const int nthreads   = omp_get_num_threads();

        MtStruct cur;
        if constexpr (NUMA) {
            numa_mt(cur, mask.size(), tid, nthreads, numa_nodes);
        } else {
            simple_mt(cur, mask.size(), tid, nthreads);
        }
    
        size_t local = 0;
    
        if constexpr (OneIsTrue) {
            for (size_t i = cur.start; i < cur.end; ++i)
                local += !mask[i];
        } else {
            for (size_t i = cur.start; i < cur.end; ++i)
                local += mask[i];
        }
    
        thread_counts[tid] = local;
    }

    size_t total = 0;
    
    for (int t = 0; t < nthreads; ++t) {
        thread_offsets[t] = total;
        total += thread_counts[t];
    }
    active_rows = total;
}




