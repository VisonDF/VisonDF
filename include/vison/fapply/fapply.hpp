#pragma once

template <typename T>
void fapply(void (&f)(T&), unsigned int &n) {
  unsigned int i = 2;
  unsigned int i2;
  bool is_found = 0;

  while (!is_found) {
    i2 = 0;
    while (i2 < matr_idx[i].size()) {
      if (n == matr_idx[i][i2]) {
        is_found = 1;
        break;
      };
      i2 += 1;
    };
    i += 1;
  };
  
  i -= 1;
  i2 = nrow * i2;

  unsigned int i3 = 0;

  if constexpr (std::is_same_v<T, bool>) {

    for (i = i2; i < (i2 + nrow); i += 1) {
      f(bool_v[i]);
      tmp_val_refv[n][i] = std::to_string(bool_v[i]);
    };
  } else if constexpr (std::is_same_v<T, IntT>) {

    for (i = i2; i < (i2 + nrow); i += 1) {
      f(int_v[i]);
      tmp_val_refv[n][i] = std::to_string(int_v[i]);
    };
  } else if constexpr (std::is_same_v<T, UIntT>) {

    for (i = i2; i < (i2 + nrow); i += 1) {
      f(uint_v[i]);
      tmp_val_refv[n][i3] = std::to_string(uint_v[i]);
      i3 += 1;
    };
  } else if constexpr (std::is_same_v<T, FloatT>) {
    for (i = i2; i < (i2 + nrow); i += 1) {
      f(dbl_v[i]);
      tmp_val_refv[n][i] = std::to_string(bool_v[i]);
    };
  } else if constexpr (std::is_same_v<T, char>) {
    for (i = i2; i < (i2 + nrow); i += 1) {
      f(chr_v[i]);
      tmp_val_refv[n][i] = std::string(1, chr_v[i]);
    };
  } else if constexpr (std::is_same_v<T, std::string>) {
    for (i = i2; i < (i2 + nrow); i += 1) {
      f(str_v[i]);
      tmp_val_refv[n][i] = str_v[i];
    };
  };
};


