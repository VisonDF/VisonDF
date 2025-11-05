#pragma once

template <typename T>
void get_col(unsigned int &x, 
                std::vector<T> &rtn_v) {
  
  rtn_v.resize(nrow);
  unsigned int i;
  unsigned int i2 = 0;

  if constexpr (std::is_same_v<T, bool>) {

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

    size_t j = i2;
    #pragma GCC ivdep
    for (i = 0; i < nrow; ++i, ++j) {
      rtn_v[i] = bool_v[j];
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

    size_t j = i2;
    #pragma GCC ivdep
    for (i = 0; i < nrow; ++i, ++j) {
      rtn_v[i] = int_v[j];
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

    size_t j = i2;
    #pragma GCC ivdep
    for (i = 0; i < nrow; ++i, ++j) {
      rtn_v[i] = uint_v[j];
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

    size_t j = i2;
    #pragma GCC ivdep
    for (i = 0; i < nrow; ++i, ++j) {
      rtn_v[i] = dbl_v[j];
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

    size_t j = i2;
    #pragma GCC ivdep
    for (i = 0; i < nrow; ++i, ++j) {
      rtn_v[i] = str_v[j];
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

    size_t j = i2;
    #pragma GCC ivdep
    for (i = 0; i < nrow; ++i, ++j) {
      rtn_v[i] = chr_v[j];
    };

  } else {
    std::cerr << "Error in (get_col), unsupported type\n";
    return;
  };

};



