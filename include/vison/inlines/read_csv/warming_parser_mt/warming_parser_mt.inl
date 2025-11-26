
template <unsigned int strt_row,
          unsigned int end_row,
          unsigned int CORES = 4,
          bool Lambda = false,
          typename F = DefaultApply>
requires FapplyFn<F, first_arg_t<F>>
inline void warming_parser_mt(std::string_view& csv_view,
                              const char delim,
                              const char str_context,
                              const size_t ncol,
                              const size_t i,
                              const std::vector<size_t>& newline_pos,
                              const bool header_name,
                              F f = F{})
{
    if (strt_row == 0 && end_row == 0) {

        int nthreads = CORES;
        size_t chunk = (nrow + nthreads - 1) / nthreads;

        std::vector<std::vector<std::vector<std::string_view>>> thread_columns(
            nthreads, std::vector<std::vector<std::string_view>>(ncol)
        );
        
        for (auto& thread : thread_columns)
            for (auto& col : thread)
                col.reserve(chunk);

        #pragma omp parallel for num_threads(CORES)
        for (int t = 0; t < nthreads; ++t) {
            size_t start_row_chunk = t * chunk;
            size_t end_row_chunk   = std::min(strt_row + chunk, static_cast<size_t>(nrow));

            // minus 1 because of the first pass
            size_t start_byte = (start_row_chunk == 0) ? i : newline_pos[start_row_chunk - 1] + 1; 
            size_t end_byte   = newline_pos[end_row_chunk - 1];
            size_t slice_size = end_byte - start_byte;

            const char* src_ptr = csv_view.data() + start_byte;
            char* local_buf = nullptr;
            posix_memalign((void**)&local_buf, 64, slice_size); // 64 makes sure the starting pointer is divisible by 64
                                                                // so at max 63 more bytes reserved, handled automatically

            const size_t V = 32;
            size_t j = 0;
            for (; j + V <= slice_size; j += V) {
                __m256i v = _mm256_loadu_si256((const __m256i*)(src_ptr + j));
                _mm256_storeu_si256((__m256i*)(local_buf + j), v);  
            }
            for (; j < slice_size; ++j) local_buf[j] = src_ptr[j];

            std::string_view chunk_view(local_buf, slice_size);
            const char* orig_base = csv_view.data();           
            size_t orig_start_byte = start_byte;                
            parse_rows_chunk_warmed<Lambda>(chunk_view, 
                                    orig_base, 
                                    orig_start_byte,
                                    thread_columns[t], 
                                    delim, 
                                    str_context, 
                                    ncol);

            free(local_buf);
        }

        // merging phase for tmp_val_refv
        #pragma omp parallel for num_threads(CORES) 
        for (size_t c = 0; c < ncol; ++c) {
            size_t total = 0;
            for (int t = 0; t < nthreads; ++t)
                total += thread_columns[t][c].size();

            auto& dst = tmp_val_refv[c];

            for (int t = 0; t < nthreads; ++t) {
                auto& src = thread_columns[t][c];
                dst.insert(dst.end(), src.begin(), src.end());
            }
        }

        if (nrow > tmp_val_refv[0].size()) {
            nrow -= 1;
        }

    } else if constexpr (strt_row != 0 && end_row != 0) {

        nrow = end_row - strt_row;
        int nthreads = CORES;
        size_t chunk = (nrow + nthreads - 1) / nthreads;

        std::vector<std::vector<std::vector<std::string_view>>> thread_columns(
            nthreads, std::vector<std::vector<std::string_view>>(ncol)
        );
        
        for (auto& thread : thread_columns)
            for (auto& col : thread)
                col.reserve(chunk);
        
        const unsigned int strt_row2 = (header_name) ? strt_row + 1 : strt_row;
        const unsigned int end_rowb = (header_name) ? end_row + 1 : end_row;

        #pragma omp parallel for num_threads(CORES)
        for (int t = 0; t < nthreads; ++t) {
            size_t start_row_chunk = t * chunk + strt_row2;
            size_t end_row_chunk   = std::min(strt_row + chunk, 
                            static_cast<size_t>(end_rowb));

            // minus 1 because of the first pass
            size_t start_byte = newline_pos[start_row_chunk - 1] + 1;
            size_t end_byte   = newline_pos[end_row_chunk   - 1];
            size_t slice_size = end_byte - start_byte;

            const char* src_ptr = csv_view.data() + start_byte; 
            char* local_buf = nullptr;
            posix_memalign((void**)&local_buf, 64, slice_size); // 64 makes sur the starting pointer is divisible by 64
                                                                // so at max 63 more bytes reserved, handled automatically

            const size_t V = 32;
            size_t j = 0;
            for (; j + V <= slice_size; j += V) {
                __m256i v = _mm256_loadu_si256((const __m256i*)(src_ptr + j));
                _mm256_storeu_si256((__m256i*)(local_buf + j), v);  
            }
            for (; j < slice_size; ++j) local_buf[j] = src_ptr[j];

            std::string_view chunk_view(local_buf, slice_size);
            const char* orig_base = csv_view.data();           
            size_t orig_start_byte = start_byte; 
            parse_rows_chunk_warmed<Lambda>(chunk_view, 
                                    orig_base, 
                                    orig_start_byte,
                                    thread_columns[t], 
                                    delim, 
                                    str_context, 
                                    ncol);

            free(local_buf);
        }

        // merging phase for tmp_val_refv
        #pragma omp parallel for num_threads(CORES) 
        for (size_t c = 0; c < ncol; ++c) {
            size_t total = 0;
            for (int t = 0; t < nthreads; ++t)
                total += thread_columns[t][c].size();

            auto& dst = tmp_val_refv[c];

            for (int t = 0; t < nthreads; ++t) {
                auto& src = thread_columns[t][c];
                dst.insert(dst.end(), src.begin(), src.end());
            }
        }

        if (nrow > tmp_val_refv[0].size()) {
            nrow -= 1;
        }

    } else if constexpr (strt_row != 0) {

        unsigned int nrow_lst = nrow;
        nrow = nrow_lst - strt_row;
        int nthreads = CORES;
        size_t chunk = (nrow + nthreads - 1) / nthreads;

        std::vector<std::vector<std::vector<std::string_view>>> thread_columns(
            nthreads, std::vector<std::vector<std::string_view>>(ncol)
        );
        
        for (auto& thread : thread_columns)
            for (auto& col : thread)
                col.reserve(chunk);
        
        const unsigned int strt_row2 = (header_name) ? strt_row + 1 : strt_row;

        #pragma omp parallel for num_threads(CORES)
        for (int t = 0; t < nthreads; ++t) {
            size_t start_row_chunk = t * chunk + strt_row2;
            size_t end_row_chunk   = std::min(strt_row + chunk, 
                            static_cast<size_t>(nrow_lst));

            // minus 1 because of the first pass
            size_t start_byte = newline_pos[start_row_chunk - 1] + 1;
            size_t end_byte   = newline_pos[end_row_chunk   - 1];
            size_t slice_size = end_byte - start_byte;

            const char* src_ptr = csv_view.data() + start_byte; 
            char* local_buf = nullptr;
            posix_memalign((void**)&local_buf, 64, slice_size); // 64 makes sur the starting pointer is divisible by 64
                                                                // so at max 63 more bytes reserved, handled automatically

            const size_t V = 32;
            size_t j = 0;
            for (; j + V <= slice_size; j += V) {
                __m256i v = _mm256_loadu_si256((const __m256i*)(src_ptr + j));
                _mm256_storeu_si256((__m256i*)(local_buf + j), v);  
            }
            for (; j < slice_size; ++j) local_buf[j] = src_ptr[j];

            std::string_view chunk_view(local_buf, slice_size);
            const char* orig_base = csv_view.data();           
            size_t orig_start_byte = start_byte; 
            parse_rows_chunk_warmed<Lambda>(chunk_view, 
                                    orig_base, 
                                    orig_start_byte,
                                    thread_columns[t], 
                                    delim, 
                                    str_context, 
                                    ncol);

            free(local_buf);
        }

        // merging phase for tmp_val_refv
        #pragma omp parallel for num_threads(CORES)
        for (size_t c = 0; c < ncol; ++c) {
            size_t total = 0;
            for (int t = 0; t < nthreads; ++t)
                total += thread_columns[t][c].size();

            auto& dst = tmp_val_refv[c];

            for (int t = 0; t < nthreads; ++t) {
                auto& src = thread_columns[t][c];
                dst.insert(dst.end(), src.begin(), src.end());
            }
        }

        if (nrow > tmp_val_refv[0].size()) {
            nrow -= 1;
        }

    } else if constexpr (end_row != 0) {

        nrow = end_row;
        int nthreads = CORES;
        size_t chunk = (nrow + nthreads - 1) / nthreads;

        std::vector<std::vector<std::vector<std::string_view>>> thread_columns(
            nthreads, std::vector<std::vector<std::string_view>>(ncol)
        );
        
        for (auto& thread : thread_columns)
            for (auto& col : thread)
                col.reserve(chunk);
        
        const unsigned int end_rowb = (header_name) ? end_row + 1 : end_row;

        #pragma omp parallel for num_threads(CORES)
        for (int t = 0; t < nthreads; ++t) {
            size_t start_row_chunk = t * chunk;
            size_t end_row_chunk   = std::min(strt_row + chunk, 
                            static_cast<size_t>(end_rowb));

            // minus 1 because of the first pass
            size_t start_byte = (start_row_chunk == 0) ? i : newline_pos[start_row_chunk - 1] + 1;
            size_t end_byte   = newline_pos[end_row_chunk - 1];
            size_t slice_size = end_byte - start_byte;

            const char* src_ptr = csv_view.data() + start_byte; 
            char* local_buf = nullptr;
            posix_memalign((void**)&local_buf, 64, slice_size); // 64 makes sur the starting pointer is divisible by 64
                                                                // so at max 63 more bytes reserved, handled automatically

            const size_t V = 32;
            size_t j = 0;
            for (; j + V <= slice_size; j += V) {
                __m256i v = _mm256_loadu_si256((const __m256i*)(src_ptr + j));
                _mm256_storeu_si256((__m256i*)(local_buf + j), v);  
            }
            for (; j < slice_size; ++j) local_buf[j] = src_ptr[j];

            std::string_view chunk_view(local_buf, slice_size);
            const char* orig_base = csv_view.data();           
            size_t orig_start_byte = start_byte; 
            parse_rows_chunk_warmed<Lambda>(chunk_view, 
                                    orig_base, 
                                    orig_start_byte,
                                    thread_columns[t], 
                                    delim, 
                                    str_context, 
                                    ncol);

            free(local_buf);
        }

        // merging phase for tmp_val_refv
        #pragma omp parallel for num_threads(CORES)
        for (size_t c = 0; c < ncol; ++c) {
            size_t total = 0;
            for (int t = 0; t < nthreads; ++t)
                total += thread_columns[t][c].size();

            auto& dst = tmp_val_refv[c];

            for (int t = 0; t < nthreads; ++t) {
                auto& src = thread_columns[t][c];
                dst.insert(dst.end(), src.begin(), src.end());
            }
        }

        if (nrow > tmp_val_refv[0].size()) {
            nrow -= 1;
        }

    }
}



