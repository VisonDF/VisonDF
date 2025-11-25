
template <unsigned int strt_row,
          unsigned int end_row,
          unsigned int CORES = 4>
inline void standard_parser_mt(std::string_view& csv_view,
                               const char delim,
                               const char str_context,
                               const size_t ncol,
                               const size_t i,
                               const std::vector<size_t>& newline_pos,
                               const bool header_name)
{
    if constexpr (strt_row == 0 && end_row == 0) {

        int nthreads = CORES;
        size_t chunk = (nrow + nthreads - 1) / nthreads;

        std::vector<std::vector<std::vector<std::string_view>>> thread_columns(
            nthreads, std::vector<std::vector<std::string_view>>(ncol)
        );
        
        for (auto& thread : thread_columns) {
            for (auto& col : thread)
                col.reserve(chunk);
        }

        #pragma omp parallel for num_threads(CORES)
        for (int t = 0; t < nthreads; ++t) {

            size_t start_row_chunk = t * chunk;
            size_t end_row_chunk   = std::min(strt_row + chunk, static_cast<size_t>(nrow));
       
            // minus 1 because of the first line pass
            size_t start_byte = (start_row_chunk == 0) ? i : newline_pos[start_row_chunk - 1] + 1;
            size_t end_byte   = newline_pos[end_row_chunk - 1];
        
            std::string_view chunk_view(csv_view.data() + start_byte, 
                                     end_byte - start_byte);

            parse_rows_chunk(chunk_view, 
                             0, 
                             chunk_view.size(), 
                             thread_columns[t], 
                             delim, 
                             str_context, 
                             ncol);
        }

        // merging phase for tmp_val_refv
        #pragma omp parallel for num_threads(CORES)
        for (size_t c = 0; c < ncol; ++c) {
            size_t total = 0;
            for (int t = 0; t < nthreads; ++t)
                total += thread_columns[t][c].size();
        
            auto& dst = tmp_val_refv[c];
            dst.reserve(dst.size() + total);  
        
            for (int t = 0; t < nthreads; ++t) {
                auto& src = thread_columns[t][c];
                dst.insert(dst.end(), src.begin(), src.end());

            }
        }

        if (nrow > tmp_val_refv[0].size()) {
          nrow -= 1;
        };

    } else if constexpr (strt_row != 0 && end_row != 0) {

        int nthreads = CORES;
        nrow = end_row - strt_row;
        size_t chunk = (nrow + nthreads - 1) / nthreads;

        std::vector<std::vector<std::vector<std::string_view>>> thread_columns(
            nthreads, std::vector<std::vector<std::string_view>>(ncol)
        );
        
        for (auto& thread : thread_columns) {
            for (auto& col : thread)
                col.reserve(chunk);
        }

        const unsigned int strt_row2 = (header_name) ? strt_row + 1 : strt_row; 
        const unsigned int end_rowb = (header_name) ? end_row + 1 : end_row;

        #pragma omp parallel for num_threads(CORES)
        for (int t = 0; t < nthreads; ++t) {

            size_t start_row_chunk = t * chunk + strt_row2;
            size_t end_row_chunk   = std::min(strt_row + chunk, 
                            static_cast<size_t>(end_rowb));
        
            // minus 1 because of the first line pass
            size_t start_byte = newline_pos[start_row_chunk - 1] + 1;
            size_t end_byte   = newline_pos[end_row_chunk - 1];
        
            std::string_view chunk_view(csv_view.data() + start_byte, 
                                     end_byte - start_byte);

            parse_rows_chunk(chunk_view, 
                             0, 
                             chunk_view.size(), 
                             thread_columns[t], 
                             delim, 
                             str_context, 
                             ncol);
        }

        // merging phase for tmp_val_refv
        #pragma omp parallel for num_threads(CORES)
        for (size_t c = 0; c < ncol; ++c) {
            size_t total = 0;
            for (int t = 0; t < nthreads; ++t)
                total += thread_columns[t][c].size();
        
            auto& dst = tmp_val_refv[c];
            dst.reserve(dst.size() + total);  
        
            for (int t = 0; t < nthreads; ++t) {
                auto& src = thread_columns[t][c];
                dst.insert(dst.end(), src.begin(), src.end());

            }
        }

        if (nrow > tmp_val_refv[0].size()) {
          nrow -= 1;
        };

    } else if constexpr (strt_row != 0) {

        int nthreads = CORES;
        unsigned int nrow_lst = nrow;
        nrow = nrow - strt_row;
        size_t chunk = (nrow + nthreads - 1) / nthreads;

        std::vector<std::vector<std::vector<std::string_view>>> thread_columns(
            nthreads, std::vector<std::vector<std::string_view>>(ncol)
        );
        
        for (auto& thread : thread_columns) {
            for (auto& col : thread)
                col.reserve(chunk);
        }

        const unsigned int strt_row2 = (header_name) ? strt_row + 1 : strt_row; 
        nrow_lst = (header_name) ? nrow_lst + 1 : nrow_lst; 

        #pragma omp parallel for num_threads(CORES)
        for (int t = 0; t < nthreads; ++t) {

            size_t start_row_chunk = t * chunk + strt_row2;
            size_t end_row_chunk   = std::min(strt_row + chunk, 
                            static_cast<size_t>(nrow_lst));
       
            // minus 1 because of the first line pass
            size_t start_byte = newline_pos[start_row_chunk - 1] + 1;
            size_t end_byte   = newline_pos[end_row_chunk - 1];
        
            std::string_view chunk_view(csv_view.data() + start_byte, 
                                     end_byte - start_byte);

            parse_rows_chunk(chunk_view, 
                            0, 
                            chunk_view.size(), 
                            thread_columns[t], 
                            delim, 
                            str_context, 
                            ncol);
        }

        // merging phase for tmp_val_refv
        #pragma omp parallel for num_threads(CORES)
        for (size_t c = 0; c < ncol; ++c) {
            size_t total = 0;
            for (int t = 0; t < nthreads; ++t)
                total += thread_columns[t][c].size();
        
            auto& dst = tmp_val_refv[c];
            dst.reserve(dst.size() + total);  
        
            for (int t = 0; t < nthreads; ++t) {
                auto& src = thread_columns[t][c];
                dst.insert(dst.end(), src.begin(), src.end());

            }
        }

        if (nrow > tmp_val_refv[0].size()) {
          nrow -= 1;
        };

    } else if constexpr (end_row != 0) {

        int nthreads = CORES;
        nrow = end_row;
        size_t chunk = (nrow + nthreads - 1) / nthreads;

        std::vector<std::vector<std::vector<std::string_view>>> thread_columns(
            nthreads, std::vector<std::vector<std::string_view>>(ncol)
        );
        
        for (auto& thread : thread_columns) {
            for (auto& col : thread)
                col.reserve(chunk);
        }

        const unsigned int end_rowb = (header_name) ? end_row + 1 : end_row; 

        #pragma omp parallel for num_threads(CORES)
        for (int t = 0; t < nthreads; ++t) {

            size_t start_row_chunk = t * chunk;
            size_t end_row_chunk   = std::min(strt_row + chunk, 
                            static_cast<size_t>(end_rowb));
        
            size_t start_byte = (start_row_chunk == 0) ? i : newline_pos[start_row_chunk - 1] + 1;
            size_t end_byte   = newline_pos[end_row_chunk - 1];
        
            std::string_view chunk_view(csv_view.data() + start_byte, 
                                     end_byte - start_byte);

            parse_rows_chunk(chunk_view, 
                             0, 
                             chunk_view.size(), 
                             thread_columns[t], 
                             delim, 
                             str_context, 
                             ncol);
        }

        #pragma omp parallel for num_threads(CORES)
        for (size_t c = 0; c < ncol; ++c) {
            size_t total = 0;
            for (int t = 0; t < nthreads; ++t)
                total += thread_columns[t][c].size();
        
            auto& dst = tmp_val_refv[c];
            dst.reserve(dst.size() + total);  
        
            for (int t = 0; t < nthreads; ++t) {
                auto& src = thread_columns[t][c];
                dst.insert(dst.end(), src.begin(), src.end());

            }
        }

        if (nrow > tmp_val_refv[0].size()) {
          nrow -= 1;
        };

    }
}


