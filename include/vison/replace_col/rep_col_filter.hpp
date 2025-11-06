#pragma once

template <typename T> void rep_col(std::vector<T> &x, 
                unsigned int &colnb,
                const std::vector<uint8_t>& mask) {

  if (x.size() != nrow) {
    std::cerr << "Error: vector length (" << x.size()
              << ") does not match nrow (" << nrow << ")\n";
    return;
  }
 
  unsigned int i;
  unsigned int i2 = 0;
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

    for (i = 0; i < nrow; ++i) {
     
        if (!mask[i]) {
          continue;
        }

        auto& vl = x[i];
        bool_v[i2 + i] = vl;
 
        char buf[buf_size];
        auto [ptr, ec] = std::to_chars(buf, buf + buf_size, 
                                       static_cast<int>(vl));

        if (ec == std::errc{}) [[likely]] {
            val_tmp[i].assign(buf, ptr);
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

    for (i = 0; i < nrow; ++i) {

        if (!mask[i]) {
          continue;
        }

        auto& vl = x[i];
        int_v[i2 + i] = vl;
 
        char buf[buf_size];
        auto [ptr, ec] = std::to_chars(buf, buf + buf_size, vl);

        if (ec == std::errc{}) [[likely]] {
            val_tmp[i].assign(buf, ptr);
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

    for (i = 0; i < nrow; ++i) {

        if (!mask[i]) {
          continue;
        }

        auto& vl = x[i];
        uint_v[i2 + i] = vl;
 
        char buf[buf_size];
        auto [ptr, ec] = std::to_chars(buf, buf + buf_size, vl);

        if (ec == std::errc{}) [[likely]] {
            val_tmp[i].assign(buf, ptr);
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

    for (i = 0; i < nrow; ++i) {

        if (!mask[i]) {
          continue;
        }

        auto& vl = x[i];
        dbl_v[i2 + i] = vl;
 
        char buf[buf_size];
        auto [ptr, ec] = std::to_chars(buf, buf + buf_size, vl);

        if (ec == std::errc{}) [[likely]] {
            val_tmp[i].assign(buf, ptr);
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

    for (i = 0; i < nrow; ++i) {
     
      if (!mask[i]) {
        continue;
      }

      str_v[i2 + i] = x[i];
      val_tmp[i] = x[i];
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

    for (i = 0; i < nrow; ++i) {

      if (!mask[i]) {
        continue;
      }

      chr_v[i2 + i] = x[i];
      val_tmp[i].assign(1, x[i]);
    };

  } else {
    std::cerr << "Error unsupported type in (replace_col)\n";
  };
};




