#pragma once

template <unsigned int strt_row = 0, 
          unsigned int end_row = 0, 
          unsigned int CORES = 1, 
          bool WARMING = 0, 
          bool MEM_CLEAN = 0,
          char TrailingChar = '0'>
void readf(std::string &file_name, char delim = ',', bool header_name = 1, char str_context = '\'') {
            
          int fd = open(file_name.c_str(), O_RDONLY);
          if (fd == -1) {
              perror("open");
              throw std::runtime_error("Failed to open file: " + file_name);
          }
          
          size_t size = lseek(fd, 0, SEEK_END);
          if (size == (size_t)-1) {
              perror("lseek");
              close(fd);
              throw std::runtime_error("Failed to determine file size: " + file_name);
          }
    
          posix_fadvise(fd, 0, 0, POSIX_FADV_WILLNEED);
          readahead(fd, 0, size);
          posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
          
          void* mapped = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
          if (mapped == MAP_FAILED) {
              perror("mmap");
              close(fd);
              throw std::runtime_error("Failed to mmap file: " + file_name);
          }
         
          madvise(mapped, size, MADV_SEQUENTIAL | MADV_WILLNEED);
          madvise(mapped, size, MADV_HUGEPAGE); // optional, if supported
    
          close(fd); 
          
          std::string_view csv_view(static_cast<const char*>(mapped), size);
                
          ncol = 1;
          std::vector<std::string> ex_vec = {};
          unsigned int i = 0;
          unsigned int verif_ncol;
          bool str_cxt = 0;
          tmp_val_refv.reserve(64);
    
          if (header_name) {
    
            bool in_quotes = false;
            size_t field_start = 0;
            
            for (; i < csv_view.size(); ++i) {
                char c = csv_view[i];
                if (c == '\n' || c == '\r') {
                  if (i + 1 < csv_view.size() && csv_view[i + 1] == '\n') {
                    ++i;
                  };
                  break;
                };
                if (c == str_context) {
                    in_quotes = !in_quotes;
                } else if (c == delim && !in_quotes) {
                    std::string_view field = csv_view.substr(field_start, i - field_start);
            
                    if (field.empty()) {
                        name_v.emplace_back("NA");
                    } else {
                        name_v.emplace_back(field);
                    }
            
                    tmp_val_refv.emplace_back(std::move(ex_vec));
                    ++ncol;
                    field_start = i + 1;
                }
            }
    
            std::string_view field = csv_view.substr(field_start, i - field_start);
            if (field.empty()) {
                name_v.emplace_back("NA");
            } else {
                name_v.emplace_back(field);
            }
            tmp_val_refv.emplace_back(std::move(ex_vec));
            ex_vec.clear();
            
          } else {
    
            bool in_quotes = false;
            size_t field_start = 0;
            
            for (; i < csv_view.size(); ++i) {
                char c = csv_view[i];
                if (c == '\n' || c == '\r') {
                  if (i + 1 < csv_view.size() && csv_view[i + 1] == '\n') {
                    ++i;
                  };
                  break;
                };
                if (c == str_context) {
                    in_quotes = !in_quotes;
                } else if (c == delim && !in_quotes) {
                    std::string_view field = csv_view.substr(field_start, i - field_start);
            
                    ex_vec.emplace_back(field);
                    tmp_val_refv.emplace_back(std::move(ex_vec));
                    ex_vec = {};
                    ++ncol;
                    field_start = i + 1;
                }
            }
     
            std::string_view field = csv_view.substr(field_start, i - field_start);
            if (field.empty()) {
                ex_vec = {"NA"};
            } else {
                ex_vec.emplace_back(field);
            }
            tmp_val_refv.emplace_back(std::move(ex_vec));
            ex_vec.clear();
            nrow += 1;
          };
          type_refv.reserve(ncol);
    
          size_t header_end = csv_view.find_first_of("\r\n");
          if (header_end == std::string_view::npos) header_end = csv_view.size();
          
          std::string_view data_view = (header_end < csv_view.size())
              ? csv_view.substr(header_end + ((header_end + 1 < csv_view.size() &&
                                               csv_view[header_end] == '\r' &&
                                               csv_view[header_end + 1] == '\n') ? 2 : 1))
              : std::string_view{};
           
          auto lines_info = simd_count_newlines(csv_view.data(), csv_view.size());
          const auto& newline_pos = lines_info.positions;
    
          nrow = lines_info.count;
    
          if (header_name) {
            nrow -= 1;
          };
    
          if (!data_view.empty() && data_view.back() != '\n' && data_view.back() != '\r') {
              ++nrow;
          }
      
          i += 1;
         
          if constexpr (CORES > 1) { 
            
            if constexpr (WARMING) {
    
              if (strt_row == 0 && end_row == 0) {
    
                int nthreads = CORES;
                size_t chunk = (nrow + nthreads - 1) / nthreads;
    
                std::vector<std::vector<std::vector<std::string_view>>> thread_columns(
                    nthreads, std::vector<std::vector<std::string_view>>(ncol)
                );
                
                for (auto& thread : thread_columns)
                    for (auto& col : thread)
                        col.reserve(chunk);
    
                #pragma omp parallel for
                for (int t = 0; t < nthreads; ++t) {
                    size_t start_row = t * chunk;
                    size_t end_row2  = std::min(start_row + chunk, static_cast<size_t>(nrow));
                    if (start_row >= end_row2) continue;
    
                    size_t start_byte = (start_row == 0) ? i : newline_pos[start_row - 1] + 1;
                    size_t end_byte   = newline_pos[end_row2 - 1];
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
    
                    std::string_view local_view(local_buf, slice_size);
                    const char* orig_base = csv_view.data();           
                    size_t orig_start_byte = start_byte;                
                    parse_rows_range_cached(local_view, orig_base, orig_start_byte,
                                thread_columns[t], delim, str_context, ncol);
    
                    free(local_buf);
                }
    
                #pragma omp parallel for 
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
    
                #pragma omp parallel for
                for (int t = 0; t < nthreads; ++t) {
                    size_t start_row = t * chunk + strt_row2;
                    size_t end_row2  = std::min(start_row + chunk, 
                                    static_cast<size_t>(end_rowb));
                    if (start_row >= end_row2) continue;
    
                    size_t start_byte = newline_pos[start_row - 1] + 1;
                    size_t end_byte   = newline_pos[end_row2 - 1];
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
    
                    std::string_view local_view(local_buf, slice_size);
                    const char* orig_base = csv_view.data();           
                    size_t orig_start_byte = start_byte;
    
                    parse_rows_range_cached(local_view, orig_base, orig_start_byte,
                                thread_columns[t], delim, str_context, ncol);
    
                    free(local_buf);
                }
    
                #pragma omp parallel for 
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
                }
    
              } else if constexpr (strt_row != 0) {
    
                unsigned int nrow_lst = nrow;
                nrow = nrow_lst - strt_row;
                nrow_lst += ((header_name) ? 1 : 0);
                int nthreads = CORES;
                size_t chunk = (nrow + nthreads - 1) / nthreads;
    
                std::vector<std::vector<std::vector<std::string_view>>> thread_columns(
                    nthreads, std::vector<std::vector<std::string_view>>(ncol)
                );
                
                for (auto& thread : thread_columns)
                    for (auto& col : thread)
                        col.reserve(chunk);
                
                const unsigned int strt_row2 = (header_name) ? strt_row + 1 : strt_row;
    
                #pragma omp parallel for
                for (int t = 0; t < nthreads; ++t) {
                    size_t start_row = t * chunk + strt_row2;
                    size_t end_row2  = std::min(start_row + chunk, 
                                    static_cast<size_t>(nrow_lst));
                    if (start_row >= end_row2) continue;
    
                    size_t start_byte = newline_pos[start_row - 1] + 1;
                    size_t end_byte   = newline_pos[end_row2 - 1];
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
    
                    std::string_view local_view(local_buf, slice_size);
                    const char* orig_base = csv_view.data();           
                    size_t orig_start_byte = start_byte;
    
                    parse_rows_range_cached(local_view, orig_base, orig_start_byte,
                                thread_columns[t], delim, str_context, ncol);
    
                    free(local_buf);
                }
    
                #pragma omp parallel for 
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
    
                #pragma omp parallel for
                for (int t = 0; t < nthreads; ++t) {
                    size_t start_row = t * chunk;
                    size_t end_row2  = std::min(start_row + chunk, 
                                    static_cast<size_t>(end_rowb));
    
                    if (start_row >= end_row2) continue; 
    
                    size_t start_byte = (start_row == 0) ? i : newline_pos[start_row - 1] + 1;
                    size_t end_byte   = newline_pos[end_row2 - 1];
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
    
                    std::string_view local_view(local_buf, slice_size);
                    const char* orig_base = csv_view.data();           
                    size_t orig_start_byte = start_byte;
    
                    parse_rows_range_cached(local_view, orig_base, orig_start_byte,
                                thread_columns[t], delim, str_context, ncol);
    
                    free(local_buf);
                }
    
                #pragma omp parallel for 
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
                }
        
              }
    
            } else if constexpr (!WARMING) {
    
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
     
                #pragma omp parallel for
                for (int t = 0; t < nthreads; ++t) {
    
                    size_t start_row = t * chunk;
                    size_t end_row2   = std::min(start_row + chunk, static_cast<size_t>(nrow));
                
                    if (start_row >= end_row2) continue;
                
                    size_t start_byte = (start_row == 0) ? i : newline_pos[start_row - 1] + 1;
                    size_t end_byte   = newline_pos[end_row2 - 1];
                
                    std::string_view subview(csv_view.data() + start_byte, 
                                             end_byte - start_byte);
    
                    parse_rows_range(subview, 
                                    0, subview.size(), 
                                    thread_columns[t], 
                                    delim, str_context, ncol);
                }
    
                #pragma omp parallel for 
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
    
                #pragma omp parallel for
                for (int t = 0; t < nthreads; ++t) {
    
                    size_t start_row = t * chunk + strt_row2;
                    size_t end_row2 = std::min(start_row + chunk, 
                                    static_cast<size_t>(end_rowb));
                
                    if (start_row >= end_row2) continue;
                
                    size_t start_byte = (start_row == 0) ? i : newline_pos[start_row - 1] + 1;
                    size_t end_byte   = newline_pos[end_row2 - 1];
                
                    std::string_view subview(csv_view.data() + start_byte, 
                                             end_byte - start_byte);
    
                    parse_rows_range(subview, 
                                    0, subview.size(), 
                                    thread_columns[t], 
                                    delim, str_context, ncol);
                }
    
                #pragma omp parallel for 
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
    
                #pragma omp parallel for
                for (int t = 0; t < nthreads; ++t) {
    
                    size_t start_row = t * chunk + strt_row2;
                    size_t end_row2 = std::min(start_row + chunk, 
                                    static_cast<size_t>(nrow_lst));
                
                    if (start_row >= end_row2) continue;
                
                    size_t start_byte = (start_row == 0) ? i : newline_pos[start_row - 1] + 1;
                    size_t end_byte   = newline_pos[end_row2 - 1];
                
                    std::string_view subview(csv_view.data() + start_byte, 
                                             end_byte - start_byte);
    
                    parse_rows_range(subview, 
                                    0, subview.size(), 
                                    thread_columns[t], 
                                    delim, str_context, ncol);
                }
    
                #pragma omp parallel for 
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
    
                #pragma omp parallel for
                for (int t = 0; t < nthreads; ++t) {
    
                    size_t start_row = t * chunk;
                    size_t end_row2 = std::min(start_row + chunk, 
                                    static_cast<size_t>(end_rowb));
                
                    if (start_row >= end_row2) continue;
                
                    size_t start_byte = (start_row == 0) ? i : newline_pos[start_row - 1] + 1;
                    size_t end_byte   = newline_pos[end_row2 - 1];
                
                    std::string_view subview(csv_view.data() + start_byte, 
                                             end_byte - start_byte);
    
                    parse_rows_range(subview, 
                                    0, subview.size(), 
                                    thread_columns[t], 
                                    delim, str_context, ncol);
                }
    
                #pragma omp parallel for 
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
    
          } else if constexpr (CORES == 1) {
    
            for (auto& col : tmp_val_refv) {
              col.reserve(nrow);
            };
    
            if constexpr (strt_row == 0 && end_row == 0) {
              
                  const char* base = csv_view.data();
                  const size_t N = csv_view.size();
              
                  bool in_quotes = false;
                  size_t field_start = i;
                  verif_ncol = 0;
    
                  size_t pos = i;
    
                  __m256i D = _mm256_set1_epi8(delim);
                  __m256i Q = _mm256_set1_epi8(str_context);
                  static const __m256i NL = _mm256_set1_epi8('\n');
                  static const __m256i CR = _mm256_set1_epi8('\r');
    
                  for (; pos + 32 <= N; ) {
        
                    _mm_prefetch(base + pos + 1024, _MM_HINT_T0);
                    
                    __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(base + pos));
    
                    int mD  = _mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, D));
                    int mQ  = _mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, Q));
                    int mNL = _mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, NL));
                    int mCR = _mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, CR));
    
                    int mNL_any = (mNL | mCR);
                    int events  = (mD | mNL_any | mQ);
    
                    while (events) {
                        int bit = __builtin_ctz(events);
                        size_t idx = pos + bit;
                        char c = base[idx];
    
                        if (c == str_context) {
                            in_quotes = !in_quotes;
                        }
                        else if (!in_quotes && c == delim) {
                            std::string_view field = csv_view.substr(field_start, idx - field_start);
    
                            tmp_val_refv[verif_ncol].emplace_back(field.empty() ? "NA" : std::string(field));
                            ++verif_ncol;
                            field_start = idx + 1;
                        }
                        else if (!in_quotes && (c == '\n' || c == '\r')) {
                            std::string_view field = csv_view.substr(field_start, idx - field_start);
    
                            tmp_val_refv[verif_ncol].emplace_back(field.empty() ? "NA" : std::string(field));
    
                            if (verif_ncol + 1 != ncol) {
                                std::cerr << "column number problem at row: " << nrow << "\n";
                                reinitiate();
                                return;
                            }
    
                            verif_ncol = 0;
    
                            size_t advance = 1;
                            if (c == '\r' && idx + 1 < N && base[idx + 1] == '\n')
                                ++advance;
    
                            field_start = idx + advance;
                            pos = idx + advance; 
                            goto next_chunk;
                        }
    
                        events &= (events - 1);
                    }
    
                    pos += 32;
                    next_chunk:
                      continue;
                }
    
                for (; pos < N; ++pos) {
                    char c = base[pos];
                    if (c == str_context) {
                        in_quotes = !in_quotes;
                    } else if (!in_quotes && c == delim) {
                        std::string_view field = csv_view.substr(field_start, pos - field_start);
                        if (field.empty()) {
                            tmp_val_refv[verif_ncol].push_back("NA");
                        } else {
                            tmp_val_refv[verif_ncol].emplace_back(field);
                        }
                        ++verif_ncol;
                        field_start = pos + 1;
                    } else if (!in_quotes && (c == '\n' || c == '\r')) {
                        if (verif_ncol + 1 != ncol) {
                            std::cerr << "column number problem in readf\n";
                            reinitiate();
                            return;
                        }
                        std::string_view field = csv_view.substr(field_start, pos - field_start);
                        if (field.empty()) {
                            tmp_val_refv[verif_ncol].push_back("NA");
                        } else {
                            tmp_val_refv[verif_ncol].emplace_back(field);
                        }
              
                        verif_ncol = 0;
              
                        if (pos + 1 < N && base[pos] == '\r' && base[pos + 1] == '\n') {
                            ++pos;
                        }
                        field_start = pos + 1;
                    }
                }
              
              } else if constexpr (strt_row != 0 && end_row != 0) {
    
                  nrow = (header_name) ? 1 : 0;
    
                  size_t count = 0;
                  for (;count < strt_row ; i += 1) {
                    if (csv_view[i] == '\n') {
                      count += 1;
                    };
                  };
    
                  const char* base = csv_view.data();
                  const size_t N = csv_view.size();
              
                  bool in_quotes = false;
                  size_t field_start = i;
                  verif_ncol = 0;
    
                  size_t pos = i;
    
                  __m256i D = _mm256_set1_epi8(delim);
                  __m256i Q = _mm256_set1_epi8(str_context);
                  static const __m256i NL = _mm256_set1_epi8('\n');
                  static const __m256i CR = _mm256_set1_epi8('\r');
    
                  const size_t end_row2 = end_row - strt_row + ((header_name) ? 1 : 0);
    
                  for (; pos + 32 <= N; ) {
        
                    __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(base + pos));
    
                    int mD  = _mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, D));
                    int mQ  = _mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, Q));
                    int mNL = _mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, NL));
                    int mCR = _mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, CR));
    
                    int mNL_any = (mNL | mCR);
                    int events  = (mD | mNL_any | mQ);
    
                    while (events) {
                        int bit = __builtin_ctz(events);
                        size_t idx = pos + bit;
                        char c = base[idx];
    
                        if (c == str_context) {
                            in_quotes = !in_quotes;
                        }
                        else if (!in_quotes && c == delim) {
                            std::string_view field = csv_view.substr(field_start, idx - field_start);
    
                            tmp_val_refv[verif_ncol].emplace_back(field.empty() ? "NA" : std::string(field));
                            ++verif_ncol;
                            field_start = idx + 1;
                        }
                        else if (!in_quotes && (c == '\n' || c == '\r')) {
                            std::string_view field = csv_view.substr(field_start, idx - field_start);
    
                            tmp_val_refv[verif_ncol].emplace_back(field.empty() ? "NA" : std::string(field));
    
                            if (verif_ncol + 1 != ncol) {
                                std::cerr << "column number problem at row: " << nrow << "\n";
                                reinitiate();
                                return;
                            }
    
                            ++nrow;
                            if (nrow == end_row2) {
                              nrow -= 1;
                              goto next_chunk2b;
                            };
                            verif_ncol = 0;
    
                            size_t advance = 1;
                            if (c == '\r' && idx + 1 < N && base[idx + 1] == '\n')
                                ++advance;
    
                            field_start = idx + advance;
                            pos = idx + advance; 
                            goto next_chunk2;
                        }
    
                        events &= (events - 1);
                    }
    
                    pos += 32;
                    next_chunk2:
                      continue;
                    next_chunk2b:
                      break;
                }
    
              } else if constexpr (strt_row != 0) {
    
                    size_t count = 0;
                    for (;count < strt_row ; i += 1) {
                      if (csv_view[i] == '\n') {
                        count += 1;
                      };
                    };
    
                    nrow = nrow - strt_row + 1;
                    
                    const char* base = csv_view.data();
                    const size_t N = csv_view.size();
                
                    bool in_quotes = false;
                    size_t field_start = i;
                    verif_ncol = 0;
    
                    size_t pos = i;
    
                    __m256i D = _mm256_set1_epi8(delim);
                    __m256i Q = _mm256_set1_epi8(str_context);
                    static const __m256i NL = _mm256_set1_epi8('\n');
                    static const __m256i CR = _mm256_set1_epi8('\r');
    
                    for (; pos + 32 <= N; ) {
        
                      __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(base + pos));
    
                      int mD  = _mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, D));
                      int mQ  = _mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, Q));
                      int mNL = _mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, NL));
                      int mCR = _mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, CR));
    
                      int mNL_any = (mNL | mCR);
                      int events  = (mD | mNL_any | mQ);
    
                      while (events) {
                          int bit = __builtin_ctz(events);
                          size_t idx = pos + bit;
                          char c = base[idx];
    
                          if (c == str_context) {
                              in_quotes = !in_quotes;
                          }
                          else if (!in_quotes && c == delim) {
                              std::string_view field = csv_view.substr(field_start, idx - field_start);
    
                              tmp_val_refv[verif_ncol].emplace_back(field.empty() ? "NA" : std::string(field));
                              ++verif_ncol;
                              field_start = idx + 1;
                          }
                          else if (!in_quotes && (c == '\n' || c == '\r')) {
                              std::string_view field = csv_view.substr(field_start, idx - field_start);
    
                              tmp_val_refv[verif_ncol].emplace_back(field.empty() ? "NA" : std::string(field));
    
                              if (verif_ncol + 1 != ncol) {
                                  std::cerr << "column number problem at row: " << nrow << "\n";
                                  reinitiate();
                                  return;
                              }
    
                              //++nrow;
                              verif_ncol = 0;
    
                              size_t advance = 1;
                              if (c == '\r' && idx + 1 < N && base[idx + 1] == '\n')
                                  ++advance;
    
                              field_start = idx + advance;
                              pos = idx + advance; 
                              goto next_chunk3;
                          }
    
                          events &= (events - 1);
                      }
    
                      pos += 32;
                      next_chunk3:
                        continue;
                  }
    
                  for (; pos < N; ++pos) {
                    char c = base[pos];
                    if (c == str_context) {
                        in_quotes = !in_quotes;
                    } else if (!in_quotes && c == delim) {
                        std::string_view field = csv_view.substr(field_start, pos - field_start);
                        if (field.empty()) {
                            tmp_val_refv[verif_ncol].push_back("NA");
                        } else {
                            tmp_val_refv[verif_ncol].emplace_back(field);
                        }
                        ++verif_ncol;
                        field_start = pos + 1;
                    } else if (!in_quotes && (c == '\n' || c == '\r')) {
                        if (verif_ncol + 1 != ncol) {
                            std::cerr << "column number problem in readf\n";
                            reinitiate();
                            return;
                        }
                        std::string_view field = csv_view.substr(field_start, pos - field_start);
                        if (field.empty()) {
                            tmp_val_refv[verif_ncol].push_back("NA");
                        } else {
                            tmp_val_refv[verif_ncol].emplace_back(field);
                        }
              
                        verif_ncol = 0;
              
                        if (pos + 1 < N && base[pos] == '\r' && base[pos + 1] == '\n') {
                            ++pos;
                        }
                        field_start = pos + 1;
                    }
                }
    
              } else if constexpr (end_row != 0) {
    
                    const char* base = csv_view.data();
                    const size_t N = csv_view.size();
                
                    bool in_quotes = false;
                    size_t field_start = i;
                    verif_ncol = 0;
    
                    nrow = (header_name) ? 1 : 0;
    
                    size_t pos = i;
    
                    __m256i D = _mm256_set1_epi8(delim);
                    __m256i Q = _mm256_set1_epi8(str_context);
                    static const __m256i NL = _mm256_set1_epi8('\n');
                    static const __m256i CR = _mm256_set1_epi8('\r');
    
                    const unsigned int end_row2 = end_row + ((header_name) ? 1 : 0);
    
                    for (; pos + 32 <= N; ) {
        
                      __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(base + pos));
    
                      int mD  = _mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, D));
                      int mQ  = _mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, Q));
                      int mNL = _mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, NL));
                      int mCR = _mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, CR));
    
                      int mNL_any = (mNL | mCR);
                      int events  = (mD | mNL_any | mQ);
    
                      while (events) {
                          int bit = __builtin_ctz(events);
                          size_t idx = pos + bit;
                          char c = base[idx];
    
                          if (c == str_context) {
                              in_quotes = !in_quotes;
                          }
                          else if (!in_quotes && c == delim) {
                              std::string_view field = csv_view.substr(field_start, idx - field_start);
    
                              tmp_val_refv[verif_ncol].emplace_back(field.empty() ? "NA" : std::string(field));
                              ++verif_ncol;
                              field_start = idx + 1;
                          }
                          else if (!in_quotes && (c == '\n' || c == '\r')) {
                              std::string_view field = csv_view.substr(field_start, idx - field_start);
    
                              tmp_val_refv[verif_ncol].emplace_back(field.empty() ? "NA" : std::string(field));
    
                              if (verif_ncol + 1 != ncol) {
                                  std::cerr << "column number problem at row: " << nrow << "\n";
                                  reinitiate();
                                  return;
                              }
    
                              ++nrow;
                              if (nrow == end_row2) {
                                nrow -= 1;
                                goto next_chunk4b;
                              };
                              verif_ncol = 0;
    
                              size_t advance = 1;
                              if (c == '\r' && idx + 1 < N && base[idx + 1] == '\n')
                                  ++advance;
    
                              field_start = idx + advance;
                              pos = idx + advance; 
                              goto next_chunk4;
                          }
    
                          events &= (events - 1);
                      }
    
                      pos += 32;
                      next_chunk4:
                        continue;
                      next_chunk4b:
                        break;
                  }
    
              };
          }
    
          madvise(mapped, size, MADV_DONTNEED);
          munmap(mapped, size);
    
          if constexpr (MEM_CLEAN) {
            for (auto& el : tmp_val_refv) {
              el.shrink_to_fit();
            };
          };
    
          type_classification<CORES, TrailingChar>();
};


