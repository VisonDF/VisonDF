#pragma once

template <bool OneIsTrue,
          bool Periodic>
inline void build_boolmask(
                           std::vector<size_t>& thread_offsets,
                           const std::vector<uint8_t>& mask,
                           const size_t inner_cores,
                           size_t& active_rows,
                           const size_t n_el,
                           [[maybe_unused]] const size_t n_el2
                          )
{

    std::vector<size_t> thread_offsets2(inner_cores);
    thread_offsets.resize(inner_cores);

    #pragma omp parallel if(inner_cores > 1) num_threads(inner_cores)
    {
        const int tid = omp_get_thread_num();
        const int nthreads   = omp_get_num_threads();

        MtStruct cur;
        if constexpr (NUMA) {
            numa_mt(cur, 
                    n_el, 
                    tid, 
                    nthreads, 
                    numa_nodes);
        } else {
            simple_mt(cur, 
                      n_el, 
                      tid, 
                      nthreads);
        }
    
        size_t local = 0;
   
        if constexpr (!Periodic) {
            if constexpr (OneIsTrue) {
                for (size_t i = cur.start; i < cur.end; ++i)
                    local += !mask[i];
            } else {
                for (size_t i = cur.start; i < cur.end; ++i)
                    local += mask[i];
            }
        } else {
            if constexpr (OneIsTrue) {
                for (size_t i = cur.start; i < cur.end; ++i)
                    local += !mask[i % n_el2];
            } else {
                for (size_t i = cur.start; i < cur.end; ++i)
                    local += mask[i % n_el2];
            }
        }
    
        thread_offsets2[tid] = local;
    }

    size_t total = thread_offsets2[0]; 
    for (int t = 1; t < inner_cores; ++t) {
        thread_offsets[t] = total;
        total             += thread_offsets2[t];
    }
    active_rows = total;
}


template <bool IdxIsTrue,
          bool Periodic>
inline void build_runs_mt(
                          std::vector<size_t>& thread_offsets,
                          std::vector<size_t>& thread_counts,
                          std::vector<uint8_t>& mask,
                          const size_t inner_cores,
                          std::vector<RunsMt>& runs_vec,
                          size_t& active_rows,
                          const unsigned int n_el,
                          const unsigned int n_el2,
                         )
{

    runs_vec.resize(n_el);
    std::vector<size_t> thread_counts2(inner_cores);
    std::vector<size_t> thread_offsets2;

    if constexpr (!IdxIsTrue) {
        mask.push_back(mask.back() + 1);
        thread_offsets2.resize(inner_cores);
        thread_offsets.resize(inner_cores);
    }

    if constexpr (!Periodic) {

        #pragma omp parallel if(inner_cores > 1) num_threads(inner_cores)
        {

            const int tid = omp_get_thread_num();
            const int nthreads   = omp_get_num_threads();

            MtStruct cur;
            if constexpr (NUMA) {
                numa_mt(cur, 
                        n_el, 
                        tid, 
                        nthreads, 
                        numa_nodes);
            } else {
                simple_mt(cur, 
                          n_el, 
                          tid, 
                          nthreads);
            }

            const size_t cur_start = cur.start;
            const size_t cur_end = cur.end;

            size_t hmn = 0;

            if constexpr (IdxIsTrue) {

                for (size_t i = cur_start; i < cur_end; ) {
                    size_t out_idx   = i;
                    size_t src_start = mask[i];
    
                    while (i + 1 < cur_end &&
                           (int)((int)mask[i + 1] - ((int)mask[i] + 1)) < 2) {
                        ++i;
                    }
                    runs_vec[cur_start + hmn] = {out_idx, src_start, i - out_idx + 1};
                    ++i;
                    ++hmn;
                }

            } else {

                size_t src_start;
                size_t out_idx = 0;

                if (cur_start > 0) {
                    src_start = mask[cur_start - 1] + 1;
                } else {
                    src_start = 0;
                    if (mask[0] > 0) {
                        while (src_start < mask[0]) ++src_start;
                        runs_vec[0] = {0, 0, src_start};
                        out_idx += src_start;
                        hmn += 1;
                    }
                }

                for (size_t i = cur_start; i < cur_end; ) {

                    while (i + 1 < cur_end &&
                           (int)((int)mask[i + 1] - ((int)mask[i] + 1)) < 2) {
                        ++i;
                        ++src_start;
                    }
                    src_start += 1;
                    i += 1;
                    const size_t ref_src_start = src_start;
                    while (src_start < mask[i]) src_start += 1;
                    const size_t len = src_start - ref_src_start;

                    runs_vec[cur_start + hmn] = {out_idx, ref_src_start, len};
                    i         += 1;
                    out_idx   += len;
                    out_idx   += 1;
                    src_start += 1;
                    hmn       += 1;
                }
                thread_offsets2[tid] = out_idx - 1;
            }
            thread_counts2[tid] = hmn;
        }

    } else {

        #pragma omp parallel if(inner_cores > 1) num_threads(inner_cores)
        {
            const int tid = omp_get_thread_num();
            const int nthreads   = omp_get_num_threads();

            MtStruct cur;
            if constexpr (NUMA) {
                numa_mt(cur, 
                        n_el, 
                        tid, 
                        nthreads, 
                        numa_nodes);
            } else {
                simple_mt(cur, 
                          n_el, 
                          tid, 
                          nthreads);
            }

            const size_t cur_start = cur.start;
            const size_t cur_end = cur.end;
            size_t hmn = 0;

            if constexpr (IdxIsTrue) {
                size_t k = cur_start % n_el2;
                for (size_t i = cur_start; i < cur_end; ) {
                    size_t out_idx   = i;
                    size_t src_start = mask[k];
    
                    while (i + 1 < cur_end &&
                           (int)((int)mask[k + 1] - ((int)mask[k] + 1)) < 2) {
                        ++i;
                        k += 1;
                        k -= (k == n_el2) * n_el2;
                    }
                    runs_vec[cur_start + hmn] = {out_idx, src_start, i - out_idx + 1};
                    ++i;
                    ++hmn;
                }
            } else {

                size_t k = cur_start % n_el2;
                size_t src_start;

                if (cur_start > 0) {
                    src_start = mask[k - 1] + 1;
                } else {
                    src_start = 0;
                    if (mask[0] > 0) {
                        while (src_start < mask[0]) ++src_start;
                        runs_vec[0] = {0, 0, src_start};
                        out_idx += src_start;
                        hmn += 1;
                    }
                }

                size_t out_idx   = 0;
                for (size_t i = cur_start; i < cur_end; ) {

                    while (i + 1 < cur_end &&
                           (int)((int)mask[k + 1] - ((int)mask[k] + 1)) < 2) {
                        ++i;
                        ++k;
                        ++src_start;
                        k -= (k == n_el2) * n_el2;
                    }
                    src_start += 1;
                    i         += 1;
                    k         += 1;
                    k -= (k == n_el2) * n_el2;
                    const size_t ref_src_start = src_start;
                    while (src_start < mask[k]) src_start += 1;
                    const size_t len = src_start - ref_src_start;

                    runs_vec[cur_start + hmn] = {out_idx, ref_src_start, len};
                    i         += 1;
                    out_idx   += len;
                    out_idx   += 1;
                    src_start += 1;
                    hmn       += 1;
                    k         += 1;
                    k -= (k == n_el2) * n_el2;
                }
                thread_offsets2[tid] = out_idx - 1;
            }
            thread_counts2[tid] = hmn;
        }
    }

    if constexpr (!IdxIsTrue) {
        mask.pop_back();
        size_t total2 = thread_offsets2[0];
        for (size_t i = 1; i < inner_cores; ++i) {
            thread_offsets[i] = total2;
            total2            += thread_offsets2[i];
        }
        active_rows = total2;
    } else {
        active_rows = n_el;
    }

    size_t total = 0; 
    for (int t = 1; t < inner_cores; ++t) {
        thread_counts[t] = total;
        total            += thread_counts2[t];
    }
}

template <bool IdxIsTrue,
          bool Periodic>
inline void build_runs(
                       std::vector<uint8_t>& mask,
                       std::vector<RunsMt>& runs_vec,
                       size_t& active_rows,
                       const size_t n_el,
                       const size_t n_el2
                      )
{

    runs_vec.reserve(n_el);

    if constexpr (!Periodic) {

        if constexpr (IdxIsTrue) {

            for (size_t i = 0; i < n_el; ) {
                size_t out_idx   = i;
                size_t src_start = mask[i];
        
                while (i + 1 < cur_end &&
                       (int)((int)mask[i + 1] - ((int)mask[i] + 1)) < 2) {
                    ++i;
                }
                runs_vec.push_back({out_idx, src_start, i - out_idx + 1});
                ++i;
            }

            active_rows = n_el;

        } else {

            mask.push_back(mask.back() + 1);

            size_t out_idx   = 0;
            size_t src_start = 0;
            if (mask[0] > 0) {
                while (src_start < mask[0]) ++src_start;
                runs_vec[0] = {0, 0, src_start};
                out_idx += src_start;
                hmn += 1;
            }

            for (size_t i = 0; i < n_el; ) {

                while (i + 1 < n_el &&
                       (int)((int)mask[i + 1] - ((int)mask[i] + 1)) < 2) {
                    ++i;
                    ++src_start;
                }
                src_start += 1;
                i += 1;
                const size_t ref_src_start = src_start;
                while (src_start < mask[i]) src_start += 1;
                const size_t len = src_start - ref_src_start;

                runs_vec.push_back({out_idx, ref_src_start, len});
                i         += 1;
                out_idx   += len;
                out_idx   += 1;
                src_start += 1;
                hmn       += 1;
            }

            active_rows = out_idx - 1;
            mask.pop_back();
        }

    } else {

        if constexpr (IdxIsTrue) {

            size_t k = 0;
            for (size_t i = 0; i < n_el; ) {
                size_t out_idx   = i;
                size_t src_start = mask[k];
        
                while (i + 1 < cur_end &&
                       (int)((int)mask[k + 1] - ((int)mask[k] + 1)) < 2) {
                    i += 1;
                    k += 1;
                    k -= (k == n_el2) * n_el2;
                }
                runs_vec.push_back({out_idx, src_start, i - out_idx + 1});
                ++i;
            }

            active_rows = n_el;

        } else {

            mask.push_back(mask.back() + 1);

            size_t k         = 0;
            size_t out_idx   = 0;
            size_t out_idx2  = 0;
            size_t src_start = 0;
            if (mask[0] > 0) {
                while (src_start < mask[0]) ++src_start;
                runs[0] = {0, 0, src_start};
                out_idx += src_start;
                hmn += 1;
            }

            for (size_t i = 0; i < n_el; ) {

                while (i + 1 < n_el &&
                       (int)((int)mask[k + 1] - ((int)mask[k] + 1)) < 2) {
                    ++i;
                    ++src_start;
                    ++k;
                    k -= (k == n_el2) * n_el2;
                }
                src_start += 1;
                i         += 1;
                k         += 1;
                k -= (k == n_el2) * n_el2;
                const size_t ref_src_start = src_start;
                while (src_start < mask[k]) src_start += 1;
                const size_t len = src_start - ref_src_start;

                runs_vec.push_back({out_idx, ref_src_start, len});
                i         += 1;
                out_idx   += len;
                out_idx   += 1;
                src_start += 1;
                hmn       += 1;
                k         += 1;
                k -= (k == n_el2) * n_el2;
            }

            active_rows = out_idx - 1;
            mask.pop_back();

        }
    }
}

template <bool IdxIsTrue,
          bool Periodic>
inline void build_runs_mt_simple(
                                 std::vector<size_t>& thread_offsets,
                                 std::vector<uint8_t>& mask,
                                 const size_t inner_cores,
                                 size_t& active_rows,
                                 const size_t n_el,
                                 const size_t n_el2
                                )
{

    if constexpr (IdxIsTrue) {
        active_rows = n_el;
        return;
    }
    
    thread_offsets.resize(inner_cores);
    std::vector<size_t> thread_offsets2(inner_cores, 0);
    mask.push_back(mask.back() + 1);

    #pragma omp parallel if(inner_cores > 1) num_threads(inner_cores)
    {
        const int tid = omp_get_thread_num();
        const int nthreads   = omp_get_num_threads();

        MtStruct cur;
        if constexpr (NUMA) {
            numa_mt(cur, 
                    n_el, 
                    tid, 
                    nthreads, 
                    numa_nodes);
        } else {
            simple_mt(cur, 
                      n_el, 
                      tid, 
                      nthreads);
        }

        const size_t cur_start = cur.start;
        const size_t cur_end = cur.end;

        size_t src_start;

        if (cur_start > 0) {
            src_start = mask[cur_start - 1] + 1;
        } else {
            src_start = 0;
            if (mask[0] > 0) {
                while (src_start < mask[0]) ++src_start;
                thread_offsets2[tid] += src_start;
            }
        }

        if constexpr (!Periodic) {
            for (size_t i = cur_start; i < cur_end; ) {

                while (i + 1 < cur_end &&
                       (int)((int)mask[i + 1] - ((int)mask[i] + 1)) < 2) {
                    ++i;
                    ++src_start;
                }
                src_start += 1;
                i         += 1;
                const size_t ref_src_start = src_start;
                while (src_start < mask[i]) src_start += 1;
                const size_t len = src_start - ref_src_start;

                thread_offsets2[tid] += len;

                i         += 1;
                src_start += 1;
            }
        } else {
            for (size_t i = cur_start, k = cur_start % n_el2; i < cur_end; ) {

                while (i + 1 < cur_end &&
                       (int)((int)mask[k + 1] - ((int)mask[k] + 1)) < 2) {
                    ++i;
                    ++k;
                    ++src_start;
                    k -= (k == n_el2) * n_el2;
                }
                src_start += 1;
                i         += 1;
                k         += 1;
                k -= (k == n_el2) * n_el2;
                const size_t ref_src_start = src_start;
                while (src_start < mask[k]) src_start += 1;
                const size_t len = src_start - ref_src_start;

                thread_offsets2[tid] += len;

                i         += 1;
                src_start += 1;
                k         += 1;
                k -= (k == n_el2) * n_el2;
            }
        }
    }

    mask.pop_back();

    size_t total = 0; 
    for (int t = 1; t < inner_cores; ++t) {
        thread_offsets[t] = total;
        total             += thread_offsets2[t];
    }

    active_rows = total;

}

template <bool IdxIsTrue,
          bool Periodic>
inline void build_runs_simple(
                              std::vector<uint8_t>& mask,
                              size_t& active_rows,
                              const size_t n_el,
                              const size_t n_el2
                             )
{

    if constexpr (IdxIsTrue) {
        active_rows = n_el;
        return;
    }
    
    mask.push_back(mask.back() + 1);
    size_t src_start  = 0;
    size_t global_len = 0;

    if (mask[0] > 0) {
        while (src_start < mask[0]) ++src_start;
        global_len += src_start;
    }

    if constexpr (!Periodic) {
        for (size_t i = 0; i < n_el; ) {

            while (i + 1 < n_el &&
                   (int)((int)mask[i + 1] - ((int)mask[i] + 1)) < 2) {
                ++i;
                ++src_start;
            }
            src_start += 1;
            i         += 1;
            const size_t ref_src_start = src_start;
            while (src_start < mask[i]) src_start += 1;
            const size_t len = src_start - ref_src_start;
            i          += 1;
            src_start  += 1;
            global_len += len;
        }
    } else {
        for (size_t i = 0, k = 0; i < n_el; ) {

            while (i + 1 < n_el &&
                   (int)((int)mask[k + 1] - ((int)mask[k] + 1)) < 2) {
                ++i;
                ++k;
                ++src_start;
                k -= (k == n_el2) * n_el2;
            }
            src_start += 1;
            i         += 1;
            k         += 1;
            k         -= (k == n_el2) * n_el2;
            const size_t ref_src_start = src_start;
            while (src_start < mask[k]) src_start += 1;
            const size_t len = src_start - ref_src_start;
            i          += 1;
            src_start  += 1;
            k          += 1;
            global_len += len;
            k          -= (k == n_el2) * n_el2;
        }
    }

    mask.pop_back();

    active_rows = global_len;

}






