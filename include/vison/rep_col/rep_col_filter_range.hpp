#pragma once

template <typename T> 
void rep_col_filter_range(std::vector<T> &x, 
                unsigned int &colnb,
                const std::vector<uint8_t>& mask,
                const unsigned int& strt_vl) {

  if (x.size() != nrow) {
    std::cerr << "Error: vector length (" << x.size()
              << ") does not match nrow (" << nrow << ")\n";
    return;
  }
 
  unsigned int i;
  unsigned int i2 = 0;
  const unsigned int end_mask = mask.size();

  if constexpr (std::is_same_v<T, bool>) {

    while (i2 < matr_idx[2].size()) {
      if (colnb == matr_idx[2][i2]) {
        break;
      };
      i2 += 1;
    };

    if (i2 == matr_idx[2].size()) {
        std::cerr << "Error: column " << colnb << " not found for this type in (replace_col)\n";
        return;
    }

    i2 = nrow * i2;

    std::vector<std::string>& val_tmp = tmp_val_refv[colnb]; 
    constexpr size_t buf_size = 2;

    for (auto& el : val_tmp) {
      el.reserve(buf_size);
    }

    for (i = 0; i < end_mask; ++i) {
     
        if (!mask[i]) {
          continue;
        }

        auto& vl = x[strt_vl + i];
        bool_v[strt_vl + i2 + i] = vl;
 
        char buf[buf_size];
        auto [ptr, ec] = std::to_chars(buf, buf + buf_size, 
                                       static_cast<int>(vl));

        if (ec == std::errc{}) [[likely]] {
            val_tmp[strt_vl + i].assign(buf, ptr);
        } else [[unlikely]] {
            std::terminate();
        }

    };

  } else if constexpr (std::is_same_v<T, IntT>) {

    while (i2 < matr_idx[3].size()) {
      if (colnb == matr_idx[3][i2]) {
        break;
      };
      i2 += 1;
    };
    
    if (i2 == matr_idx[3].size()) {
        std::cerr << "Error: column " << colnb << " not found for this type in (replace_col)\n";
        return;
    }

    i2 = nrow * i2;

    std::vector<std::string>& val_tmp = tmp_val_refv[colnb]; 
    constexpr size_t buf_size = max_chars_needed<T>();

    for (auto& el : val_tmp) {
      el.reserve(buf_size);
    }

    for (i = 0; i < end_mask; ++i) {

        if (!mask[i]) {
          continue;
        }

        auto& vl = x[strt_vl + i];
        int_v[i2 + strt_vl + i] = vl;
 
        char buf[buf_size];
        auto [ptr, ec] = std::to_chars(buf, buf + buf_size, vl);

        if (ec == std::errc{}) [[likely]] {
            val_tmp[strt_vl + i].assign(buf, ptr);
        } else [[unlikely]] {
            std::terminate();
        }

    };

  } else if constexpr (std::is_same_v<T, UIntT>) {

    while (i2 < matr_idx[4].size()) {
      if (colnb == matr_idx[4][i2]) {
        break;
      };
      i2 += 1;
    };
    
    if (i2 == matr_idx[4].size()) {
        std::cerr << "Error: column " << colnb << " not found for this type in (replace_col)\n";
        return;
    }

    i2 = nrow * i2;

    std::vector<std::string>& val_tmp = tmp_val_refv[colnb]; 
    constexpr size_t buf_size = max_chars_needed<T>();

    for (auto& el : val_tmp) {
      el.reserve(buf_size);
    }

    for (i = 0; i < end_mask; ++i) {

        if (!mask[i]) {
          continue;
        }

        auto& vl = x[strt_vl + i];
        uint_v[i2 + strt_vl + i] = vl;
 
        char buf[buf_size];
        auto [ptr, ec] = std::to_chars(buf, buf + buf_size, vl);

        if (ec == std::errc{}) [[likely]] {
            val_tmp[strt_vl + i].assign(buf, ptr);
        } else [[unlikely]] {
            std::terminate();
        }

    };

  } else if constexpr (std::is_same_v<T, FloatT>) {

    while (i2 < matr_idx[5].size()) {
      if (colnb == matr_idx[5][i2]) {
        break;
      };
      i2 += 1;
    };
    
    if (i2 == matr_idx[5].size()) {
        std::cerr << "Error: column " << colnb << " not found for this type in (replace_col)\n";
        return;
    }

    i2 = nrow * i2;
    
    std::vector<std::string>& val_tmp = tmp_val_refv[colnb]; 
    constexpr size_t buf_size = max_chars_needed<T>();

    for (auto& el : val_tmp) {
      el.reserve(buf_size);
    }

    for (i = 0; i < end_mask; ++i) {

        if (!mask[i]) {
          continue;
        }

        auto& vl = x[strt_vl + i];
        dbl_v[i2 + strt_vl + i] = vl;
 
        char buf[buf_size];
        auto [ptr, ec] = std::to_chars(buf, buf + buf_size, vl);

        if (ec == std::errc{}) [[likely]] {
            val_tmp[strt_vl + i].assign(buf, ptr);
        } else [[unlikely]] {
            std::terminate();
        }

    };

  } else if constexpr (std::is_same_v<T, std::string>) {

    while (i2 < matr_idx[0].size()) {
      if (colnb == matr_idx[0][i2]) {
        break;
      };
      i2 += 1;
    };
   
    if (i2 == matr_idx[0].size()) {
        std::cerr << "Error: column " << colnb << " not found for std::string in (replace_col)\n";
        return;
    }

    i2 = nrow * i2;

    std::vector<std::string>& val_tmp = tmp_val_refv[colnb];

    for (i = 0; i < end_mask; ++i) {
     
      if (!mask[i]) {
        continue;
      }

      const std::string& val = x[strt_vl + i];
      str_v[i2 + strt_vl + i] = val;
      val_tmp[strt_vl + i] = val;
    };

  } else if constexpr (std::is_same_v<T, char>) {

    while (i2 < matr_idx[1].size()) {
      if (colnb == matr_idx[1][i2]) {
        break;
      };
      i2 += 1;
    }; 

    if (i2 == matr_idx[1].size()) {
        std::cerr << "Error: column " << colnb << " not found for this type in (replace_col)\n";
        return;
    }

    i2 = nrow * i2;

    std::vector<std::string>& val_tmp = tmp_val_refv[colnb];

    for (i = 0; i < end_mask; ++i) {

      if (!mask[i]) {
        continue;
      }

      const char& val = x[strt_vl + i];
      chr_v[i2 + strt_vl + i] = val;
      val_tmp[strt_vl + i].assign(1, val);
    };

  } else {
    std::cerr << "Error unsupported type in (replace_col)\n";
  };
};




