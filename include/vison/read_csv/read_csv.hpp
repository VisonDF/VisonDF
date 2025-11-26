#pragma once

template <unsigned int strt_row = 0, 
          unsigned int end_row = 0, 
          unsigned int CORES = 1, 
          bool WARMING = 0, 
          bool MEM_CLEAN = 0,
          char TrailingChar = '0'>
void read_csv(std::string &file_name, char delim = ',', bool header_name = 1, char str_context = '\'') {
            
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
   
    // First read pass -> get the number of cols
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
    // end of the first read pass //
   
    // here we search for the next break line pattern, 
    // because we already analyzed the first line
    size_t header_end = csv_view.find_first_of("\r\n");
    if (header_end == std::string_view::npos) header_end = csv_view.size(); //if not found
   
    // so now we'll return the string_view, except the alredy passed line
    std::string_view data_view = (header_end < csv_view.size())
        ? csv_view.substr(header_end + ((header_end + 1 < csv_view.size() &&
                                         csv_view[header_end] == '\r'     &&
                                         csv_view[header_end + 1] == '\n') ? 2 : 1))
        : std::string_view{}; 
     
    auto lines_info = simd_count_newlines(csv_view.data(), csv_view.size());
    const std::vector<size_t>& newline_pos = lines_info.positions; //stores next newlines pos
    
    nrow = lines_info.count;
    
    if (header_name) {
      nrow -= 1;
    };
    
    i += 1; // because already covered first breakline with first read pass
    
    for (auto& col : tmp_val_refv) {
      col.reserve(nrow);
    };

    if constexpr (CORES > 1) { 
      
      if constexpr (WARMING) {
   
          warming_parser_mt<strt_row, end_row, CORES> (csv_view, 
                                                       delim, 
                                                       str_context, 
                                                       ncol,
                                                       i,
                                                       newline_pos,
                                                       header_name);
            
      } else if constexpr (!WARMING) {

          standard_parser_mt<strt_row, end_row, CORES>(csv_view, 
                                                       delim, 
                                                       str_context, 
                                                       ncol,
                                                       i,
                                                       newline_pos,
                                                       header_name);

      }
    
    } else if constexpr (CORES == 1) {

          standard_parser<strt_row, end_row>(csv_view, 
                                             delim, 
                                             str_context, 
                                             ncol,
                                             i,
                                             newline_pos,
                                             header_name);

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


