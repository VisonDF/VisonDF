#pragma once

void rm_col(std::vector<unsigned int> &nbcolv) {
  unsigned int i;
  unsigned int i2;
  bool is_found;
  for (int nbcol : nbcolv) {
    if (nbcol >= ncol) {
      std::cout << "The column does not exist\n";
      return;
    };
    i = 0;
    is_found = 0;
    while (!is_found) {
      i2 = 0;
      while (i2 < matr_idx[i].size()) {
        if (nbcol == matr_idx[i][i2]) {
          is_found = 1;
          break;
        };
        i2 += 1;
      };
      i += 1;
    };
    i-= 1;
    name_v.erase(name_v.begin() + nbcol);
    matr_idx[i].erase(matr_idx[i].begin() + i2);
    tmp_val_refv.erase(tmp_val_refv.begin() + nbcol);
    type_refv.erase(type_refv.begin() + nbcol);
    i2 = nrow * i2;
    if (i == 0) {
      str_v.erase(str_v.begin() + i2, str_v.begin() + i2 + nrow);
    } else if (i == 1) {
      chr_v.erase(chr_v.begin() + i2, chr_v.begin() + i2 + nrow);
    } else if (i == 2) {
      bool_v.erase(bool_v.begin() + i2, bool_v.begin() + i2 + nrow);
    } else if (i == 3) {
      int_v.erase(int_v.begin() + i2, int_v.begin() + i2 + nrow);
    } else if (i == 4) {
      uint_v.erase(uint_v.begin() + i2, uint_v.begin() + i2 + nrow);
    } else if (i == 5) {
      dbl_v.erase(dbl_v.begin() + i2, dbl_v.begin() + i2 + nrow);
    };
    ncol -= 1;
  };
};


