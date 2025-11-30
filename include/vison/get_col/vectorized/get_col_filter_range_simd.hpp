#pragma once

template <typename T, bool IsBool = false>
void get_col_filter_range_simd(unsigned int &x, 
                               std::vector<T> &rtn_v,
                               const std::vector<uint8_t> &mask,
                               const unsigned int strt_vl) {
  
    const unsigned int n_el = mask.size();
  
    auto find_col_base = [&](auto &idx_vec) -> size_t {
        size_t pos = 0;
        while (pos < idx_vec.size() && idx_vec[pos] != x)
            ++pos;
  
        if (pos == idx_vec.size()) {
            std::cerr << "Error in (get_col), no column found\n";
            return size_t(-1);
        }
        return pos;
    };

    size_t active_count = 0;
    for (size_t i = 0; i < nrow; ++i)
        active_count += mask[i] != 0;
  
    rtn_v.resize(active_count);

    if constexpr (std::is_same_v<T, std::string>)
        const size_t pos_base = find_col_base(matr_idx[0]);
        const std::vector<std::string>& src = str_v[pos_idx];
        size_t out_idx = 0;
        for (size_t i = 0; i < n_el; ++i) {
            if (!mask[i]) { continue; }
            rtn_v[out_idx++] = src[strt_vl + i];
        }

    } else if constexpr (std::is_same_v<T, CharT>) {
        const size_t pos_base = find_col_base(matr_idx[1]);
        const std::vector<CharT>& src = chr_v[pos_base];
        size_t out_idx = 0;
        for (size_t i = 0; i < n_el; ++i) {
            if (!mask[i]) { continue; }
            memcpy(rtn_v[out_idx++].data(), 
                   src[strt_vl + i].data(), 
                   sizeof(CharT));
        }

    } else if constexpr (IsBool) {
        const size_t pos_base = find_col_base(matr_idx[2]);
            get_filtered_col_8<T>(bool_v[pos_base], 
                                  rtn_v,
                                  mask,
                                  strt_vl,
                                  n_el);
       
    } else if constexpr (std::is_same_v<T, IntT>) {
        const size_t pos_base = find_col_base(matr_idx[3]);
        if constexpr (sizeof(T) == 1) {
            get_filtered_col_8<T>(int_v[pos_base], 
                                  rtn_v,
                                  mask,
                                  strt_vl,
                                  n_el);
        } else if constexpr (sizeof(T) == 2) {
            get_filtered_col_16<T>(int_v[pos_base], 
                                   rtn_v,
                                   mask,
                                   strt_vl,
                                   n_el);
        } else if constexpr (sizeof(T) == 4) {
            get_filtered_col_32<T>(int_v[pos_base], 
                                   rtn_v,
                                   mask,
                                   strt_vl,
                                   n_el);
        } else if constexpr (sizeof(T) == 8) {
            get_filtered_col_64<T>(int_v[pos_base], 
                                   rtn_v,
                                   mask,
                                   strt_vl,
                                   n_el);
        }
  
    } else if constexpr (std::is_same_v<T, UIntT>) { 
        const size_t pos_base = find_col_base(matr_idx[4]);
        if constexpr (sizeof(T) == 1) {
            get_filtered_col_8<T>(uint_v[pos_base], 
                                  rtn_v,
                                  mask,
                                  strt_vl,
                                  n_el);
        } else if constexpr (sizeof(T) == 2) {
            get_filtered_col_16<T>(uint_v[pos_base], 
                                   rtn_v,
                                   mask,
                                   strt_vl,
                                   n_el);
        } else if constexpr (sizeof(T) == 4) {
            get_filtered_col_32<T>(uint_v[pos_base], 
                                   rtn_v,
                                   mask,
                                   strt_vl,
                                   n_el);
        } else if constexpr (sizeof(T) == 8) {
            get_filtered_col_64<T>(uint_v[pos_base], 
                                   rtn_v,
                                   mask,
                                   strt_vl,
                                   n_el);
        }

    } else if constexpr (std::is_same_v<T, FloatT>) {
        const size_t pos_base = find_col_base(matr_idx[5]);
        if constexpr (sizeof(T) == 4) {
            get_filtered_col_32<T>(dbl_v[pos_base], 
                                   rtn_v,
                                   mask,
                                   strt_vl,
                                   n_el);
        } else if constexpr (sizeof(T) == 8) {
            get_filtered_col_64<T>(dbl_v[pos_base], 
                                   rtn_v,
                                   mask,
                                   strt_vl,
                                   n_el);
        }
  
    } else {
      std::cerr << "Error in (get_col), unsupported type\n";
      return;
    };

};


