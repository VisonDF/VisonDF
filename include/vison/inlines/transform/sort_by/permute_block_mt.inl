#pragma once

template <class T, 
          unsigned int CORES = 4,
          bool Simd = true,
          bool InnerThreads = false>
inline void permute_block_mt(
    std::vector<T>& storage,                               
    std::vector<std::vector<std::string>>& tmp_val_refv,   
    const std::vector<unsigned int>& matr_idx_k,           
    const std::vector<size_t>& idx,
    size_t nrow)
{

    if (matr_idx_k.empty())
        return;

    std::vector<T> tmp_storage(storage.size());

    std::vector<std::pair<unsigned int, unsigned int>> matr_idx_pair(matr_idx_k.size());
    size_t i3 = 0;

    #pragma unroll
    for (const auto& el : matr_idx_k)  {
      matr_idx_pair[i3] = std::make_pair(el, i3);
      i3 += 1;
    }

    if constexpr (InnerThreads) {

        int HW = CORES;
        
        int OUTER_THREADS = std::min<int>(matr_idx_pair.size(), HW);
        
        int INNER_THREADS = (HW + OUTER_THREADS - 1) / OUTER_THREADS;
        if (INNER_THREADS < 1) INNER_THREADS = 1;
        if (INNER_THREADS > nrow) INNER_THREADS = nrow;
        if (OUTER_THREADS * INNER_THREADS > HW)
            INNER_THREADS = HW / OUTER_THREADS;

        std::vector<std::vector<std::string>> thread_local_results(OUTER_THREADS, 
                    std::vector<std::string>(nrow));

        #pragma omp parallel for num_threads(OUTER_THREADS) schedule(static)
        for (size_t i = 0; i < matr_idx_pair.size(); ++i) { 
            
            const auto& [el, i2] = matr_idx_pair[i];
            const size_t pos_vl = i2 * nrow;

            auto& ref_row = tmp_val_refv[el];

            int thread_id = omp_get_thread_num();
            std::vector<std::string>& local_results = thread_local_results[thread_id];

            auto* dst_col = local_results.data();
            auto* src_col = ref_row.data();
            auto* dst_all = tmp_storage.data() + pos_vl;
            auto* src_all = storage.data() + pos_vl;

            if constexpr (Simd) {

                #pragma omp parallel num_threads(INNER_THREADS)
                {

                    #pragma omp for schedule(static)
                    for (size_t r = 0; r < nrow; ++r) {
                        dst_col[r] = std::move(src_col[idx[r]]);  
                    }

                    #pragma omp for schedule(static)
                    for (size_t r = 0; r < nrow; ++r) {
                        #pragma omp simd // here in fact the vectorization happens 
                                         // for the current thread region
                        for (int dummy = 0; dummy < 1; ++dummy) {
                            dst_all[r] = src_all[idx[r]];
                        }
                    }

                }


            } else if constexpr (!Simd) {
                
                #pragma omp parallel for num_threads(INNER_THREADS) schedule(static)
                for (size_t r = 0; r < nrow; ++r) {
                    const size_t pos_vl2 = idx[r];

                    dst_col[r] = std::move(src_col[pos_vl2]);  
                    dst_all[r] = std::move(src_all[pos_vl2]);   
                }

            }

            ref_row.swap(local_results);
        }

    } else if constexpr (!InnerThreads) {

        std::vector<std::vector<std::string>> thread_local_results(CORES, 
                    std::vector<std::string>(nrow));

        // index iteration because openMP loves it
        #pragma omp parallel for num_threads(CORES) schedule(static)
        for (size_t i = 0; i < matr_idx_pair.size(); ++i) { 
            
            const auto& [el, i2] = matr_idx_pair[i];
            const size_t pos_vl = i2 * nrow;

            auto& ref_row = tmp_val_refv[el];

            int thread_id = omp_get_thread_num();
            std::vector<std::string>& local_results = thread_local_results[thread_id];

            auto* dst_col = local_results.data();
            auto* src_col = ref_row.data();
            auto* dst_all = tmp_storage.data() + pos_vl;
            auto* src_all = storage.data() + pos_vl;

            if constexpr (Simd) {

                for (size_t r = 0; r < nrow; ++r) {
                    dst_col[r] = std::move(src_col[idx[r]]);  
                }

                #pragma omp simd
                for (size_t r = 0; r < nrow; ++r) {
                    dst_all[r] = src_all[idx[r]];
                }

            } else if constexpr (!Simd) {

                for (size_t r = 0; r < nrow; ++r) {
                    const size_t pos_vl2 = idx[r];

                    dst_col[r] = std::move(src_col[pos_vl2]);  
                    dst_all[r] = std::move(src_all[pos_vl2]);   
                }

            }

            ref_row.swap(local_results);
        }

    }

    storage.swap(tmp_storage);
}


