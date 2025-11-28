#pragma once

template <typename T, bool IsBool = false>
void get_col_filter_range_simd(unsigned int &x, 
                         std::vector<T> &rtn_v,
                         const std::vector<uint8_t> &mask,
                         const unsigned int& strt_vl) {
  
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

    if constexpr (std::is_same_v<T, std::string>) {

        size_t i2 = 0;
        while (i2 < matr_idx[0].size()) {
            if (x == matr_idx[0][i2]) { break; };
            i2 += 1;
        };
  
        if (i2 == matr_idx[2].size()) {
          std::cerr << "Error in (get_col), no column found\n";
          return;
        };
  
        size_t active_count = 0;
        for (size_t i = 0; i < nrow; ++i)
            active_count += mask[i] != 0;
  
        rtn_v.resize(active_count);
        const std::vector<std::string>& src = str_v[i2];

        for (size_t i = 0; i < n_el; ++i) {
            if (!mask[i]) { continue; }
            rtn_v[i] = src[strt_vl + i];
        };

    } else if constexpr (std::is_same_v<T, CharT>) {
        const size_t pos_base = find_col_base(matr_idx[1]);
        get_filtered_col_general<T>(chr_v[pos_base],
                                    rtn_v,
                                    mask,
                                    strt_vl);

    } else if constexpr (IsBool) {
        const size_t pos_base = find_col_base(matr_idx[2]);
        get_filtered_col_general<T>(bool_v[pos_base],
                                    rtn_v,
                                    mask,
                                    strt_vl);
  
    } else if constexpr (std::is_same_v<T, IntT>) {
        const size_t pos_base = find_col_base(matr_idx[3]);
        get_filtered_col_general<T>(int_v[pos_base],
                                    rtn_v,
                                    mask,
                                    strt_vl);
  
    } else if constexpr (std::is_same_v<T, UIntT>) { 
        const size_t pos_base = find_col_base(matr_idx[4]);
        get_filtered_col_general<T>(uint_v[pos_base],
                                    rtn_v,
                                    mask,
                                    strt_vl);
  
    } else if constexpr (std::is_same_v<T, FloatT>) {
        const size_t pos_base = find_col_base(matr_idx[5]);
        get_filtered_col_general<T>(dbl_v[pos_base],
                                    rtn_v,
                                    mask,
                                    strt_vl);
  
    } else {
      std::cerr << "Error in (get_col), unsupported type\n";
      return;
    };

};


