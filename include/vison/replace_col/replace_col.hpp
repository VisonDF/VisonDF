#pragma once

template <typename T> void replace_col(std::vector<T> &x, unsigned int &colnb) {

  if (x.size() != nrow) {
    std::cerr << "Error: vector length (" << x.size()
              << ") does not match nrow (" << nrow << ")\n";
    return;
  }

  unsigned int i = 2;
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

    for (i = 0; i < nrow; ++i) {
      bool_v[i2 + i] = x[i];
      tmp_val_refv[colnb][i] = std::to_string(x[i]);
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

    for (i = 0; i < nrow; ++i) {
      int_v[i2 + i] = x[i];
      tmp_val_refv[colnb][i] = std::to_string(x[i]);
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

    for (i = 0; i < nrow; ++i) {
      uint_v[i2 + i] = x[i];
      tmp_val_refv[colnb][i] = std::to_string(x[i]);
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

    for (i = 0; i < nrow; ++i) {
      dbl_v[i2 + i] = x[i];
      tmp_val_refv[colnb][i] = std::to_string(x[i]);
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

    for (i = 0; i < nrow; ++i) {
      str_v[i2 + i] = x[i];
      tmp_val_refv[colnb][i] = x[i];
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

    for (i = 0; i < nrow; ++i) {
      chr_v[i2 + i] = x[i];
      tmp_val_refv[colnb][i] = std::string(1, x[i]);
    };

  } else {
    std::cerr << "Error unsupported type in (replace_col)\n";
  };
};


