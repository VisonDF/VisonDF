#pragma once

template <typename T, bool IsBool = false>
void get_col_filter_idx(unsigned int &x, 
                std::vector<T> &rtn_v,
                std::vector<unsigned int> &mask) {
  
  const unsigned int n_el = mask.size();
  rtn_v.resize(n_el);
  unsigned int i;
  unsigned int i2 = 0;

  if constexpr (IsBool) {

    while (i2 < matr_idx[2].size()) {
      if (x == matr_idx[2][i2]) {
        break;
      };
      i2 += 1;
    };

    if (i2 == matr_idx[2].size()) {
      std::cerr << "Error in (get_col), no column found\n";
      return;
    };

    i2 = nrow * i2;

    for (i = 0; i < n_el; ++i) {
      const size_t pos_idx = i2 + mask[i];
      rtn_v[i] = bool_v[pos_idx];
    };

  } else if constexpr (std::is_same_v<T, IntT>) {

    while (i2 < matr_idx[3].size()) {
      if (x == matr_idx[3][i2]) {
        break;
      };
      i2 += 1;
    };

    if (i2 == matr_idx[3].size()) {
      std::cerr << "Error in (get_col), no column found\n";
      return;
    };

    i2 = nrow * i2;
    
    for (i = 0; i < n_el; ++i) {
      const size_t pos_idx = i2 + mask[i];
      rtn_v[i] = int_v[pos_idx];
    };
    
  } else if constexpr (std::is_same_v<T, UIntT>) {

    while (i2 < matr_idx[4].size()) {
      if (x == matr_idx[4][i2]) {
        break;
      };
      i2 += 1;
    };

    if (i2 == matr_idx[4].size()) {
      std::cerr << "Error in (get_col), no column found\n";
      return;
    };

    i2 = nrow * i2;

    for (i = 0; i < n_el; ++i) {
      const size_t pos_idx = i2 + mask[i];
      rtn_v[i] = uint_v[pos_idx];
    };

  } else if constexpr (std::is_same_v<T, FloatT>) {

    while (i2 < matr_idx[5].size()) {
      if (x == matr_idx[5][i2]) {
        break;
      };
      i2 += 1;
    };

    if (i2 == matr_idx[5].size()) {
      std::cerr << "Error in (get_col), no column found\n";
      return;
    };

    i2 = nrow * i2;

    for (i = 0; i < n_el; ++i) {
      const size_t pos_idx = i2 + mask[i];
      rtn_v[i] = dbl_v[pos_idx];
    };

  } else if constexpr (std::is_same_v<T, std::string>) {

    while (i2 < matr_idx[0].size()) {
      if (x == matr_idx[0][i2]) {
        break;
      };
      i2 += 1;
    };

    if (i2 == matr_idx[0].size()) {
      std::cerr << "Error in (get_col), no column found\n";
      return;
    };

    i2 = nrow * i2;

    for (i = 0; i < n_el; ++i) {
      const size_t pos_idx = i2 + mask[i];
      rtn_v[i] = str_v[pos_idx];
    };

  } else if constexpr (std::is_same_v<T, char>) {

    while (i2 < matr_idx[1].size()) {
      if (x == matr_idx[1][i2]) {
        break;
      };
      i2 += 1;
    };

    if (i2 == matr_idx[1].size()) {
      std::cerr << "Error in (get_col), no column found\n";
      return;
    };

    i2 = nrow * i2;

    for (i = 0; i < n_el; ++i) {
      const size_t pos_idx = i2 + mask[i];
      rtn_v[i] = chr_v[pos_idx];
    };

  } else {
    std::cerr << "Error in (get_col), unsupported type\n";
    return;
  };

};


