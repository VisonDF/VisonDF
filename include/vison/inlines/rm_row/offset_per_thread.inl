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


template <bool IdxIsTrue>
inline void idx_offset_per_thread_mt(std::vector<std::pair<size_t, size_t>>& thread_offsets,
                                     std::vector<size_t>& thread_counts,
                                     std::vector<uint8_t>& mask,
                                     const size_t inner_cores,
                                     size_t& active_rows,
                                     const size_t local_nrow,
                                     std::vector<RunsMt>& runs)
{

    thread_offsets.resize(inner_cores);

    if constexpr (IdxIsTrue) {

        runs.resize(mask.size());

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

            const size_t cur_start = cur.start;
            const size_t cur_end = cur.end;

            thread_offsets[tid].first = cur_start;
            size_t hmn = 0;

            for (size_t i = cur_start; i < cur_end; ) {
                size_t out_idx   = i;
                size_t src_start = mask[i];
    
                while (i + 1 < cur_end &&
                       mask[i + 1] == mask[i] + 1) {
                    ++i;
                }
                runs[cur_start + hmn] = {out_idx, src_start, i - out_idx + 1};
                ++i;
                ++hmn;
            }

            thread_offsets[tid].second = hmn;

        }

    } else {

        mask.push_back(mask.back() + 1);
        const size_t n_el = local_nrow - mask.size();
        runs.resize(n_el);

        thread_counts.resize(inner_cores);

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

            const size_t cur_start = cur.start;
            const size_t cur_end = cur.end;

            thread_offsets[tid].first = cur_start;

            size_t hmn = 0;
            size_t src_start;
            if (cur_start > 0) {
                src_start = mask[cur_start - 1] + 1;
            } else {
                src_start = 0;
                if (mask[0] > 0) {
                    while (src_start < mask[0]) ++src_start;
                    runs[0] = {0, 0, src_start};
                    out_idx += src_start;
                    hmn += 1;
                }
            }

            size_t out_idx   = 0;
            for (size_t i = cur_start; i < cur_end; ) {

                while (i + 1 < cur_end &&
                       mask[i + 1] == mask[i] + 1) {
                    ++i;
                    ++src_start;
                }
                src_start += 1;
                i += 1;
                const size_t ref_src_start = src_start;
                while (src_start < mask[i]) src_start += 1;
                const size_t len = src_start - ref_src_start;

                runs[cur_start + hmn] = {out_idx, ref_src_start, len};
                i         += 1;
                out_idx   += len;
                src_start += 1;
                hmn       += 1;
            }
            thread_offsets[tid].second = hmn;
            thread_counts[tid] = out_idx;
        }

        mask.pop_back();

    }

    size_t total = 0;
    for (auto& el : thread_offsets)
        total += el.second;
    active_nrows = total;

    if constexpr (!IdxIsTrue) {
        size_t delta = 0;
        for (size_t i = 1; i < inner_threads; ++i) {
            delta += thread_counts[i - 1];
            thread_counts[i] = delta;
        }
    }

}

template <bool IdxIsTrue>
inline void idx_offset_per_thread(std::vector<uint8_t>& mask,
                                  size_t& active_rows,
                                  const size_t local_nrow,
                                  std::vector<RunsMt>& runs)
{

    thread_offsets.resize(inner_cores);

    if constexpr (IdxIsTrue) {

        runs.reserve(mask.size());

        for (size_t i = cur_start; i < cur_end; ) {
            size_t out_idx   = i;
            size_t src_start = mask[i];
    
            while (i + 1 < cur_end &&
                   mask[i + 1] == mask[i] + 1) {
                ++i;
            }
            runs.push_back({out_idx, src_start, i - out_idx + 1});
            ++i;
        }

        active_rows = mask.size();

    } else {

        mask.push_back(mask.back() + 1);
        size_t src_start = 0;
        if (mask[0] > 0) {
            while (src_start < mask[0]) ++src_start;
            runs[0] = {0, 0, src_start};
            out_idx += src_start;
            hmn += 1;
        }

        size_t out_idx   = 0;
        for (size_t i = 0; i < mask.size(); ) {

            while (i + 1 < mask.size() &&
                   mask[i + 1] == mask[i] + 1) {
                ++i;
                ++src_start;
            }
            src_start += 1;
            i += 1;
            const size_t ref_src_start = src_start;
            while (src_start < mask[i]) src_start += 1;
            const size_t len = src_start - ref_src_start;

            runs[cur_start + hmn] = {out_idx, ref_src_start, len};
            i         += 1;
            out_idx   += len;
            src_start += 1;
            hmn       += 1;
        }

        mask.pop_back();

        active_rows = local_nrow - mask.size();

    }

}







