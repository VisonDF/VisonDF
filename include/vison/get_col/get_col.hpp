#pragma once

template <typename T>
void get_col(unsigned int &x, 
                std::vector<T> &rtn_v) {
  rtn_v.reserve(nrow);
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

    for (i = 0; i < nrow; ++i) {
      rtn_v.push_back(bool_v[i2 + i]);
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

    for (i = 0; i < nrow; ++i) {
      rtn_v.push_back(int_v[i2 + i]);
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

    for (i = 0; i < nrow; ++i) {
      rtn_v.push_back(uint_v[i2 + i]);
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

    for (i = 0; i < nrow; ++i) {
      rtn_v.push_back(dbl_v[i2 + i]);
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

    for (i = 0; i < nrow; ++i) {
      rtn_v.push_back(str_v[i2 + i]);
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

    for (i = 0; i < nrow; ++i) {
      rtn_v.push_back(chr_v[i2 + i]);
    };

  } else {
    std::cerr << "Error in (get_col), unsupported type\n";
    return;
  };

};



