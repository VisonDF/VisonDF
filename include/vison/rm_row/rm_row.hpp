#pragma once

void rm_row(std::vector<unsigned int> &x) {
  unsigned int i;
  unsigned int cnt = 0;
  std::vector<unsigned int> alrd_v = {0, 0, 0, 0, 0, 0};
  for (unsigned int i2 = nrow - 1; i2 > 0; --i2) {
    if (i2 == x[cnt]) {
      for (i = 0; i < ncol; ++i) {
        tmp_val_refv[i].erase(tmp_val_refv[i].begin() + i2);
        if (type_refv[i] == 's') {
          str_v.erase(str_v.begin() + alrd_v[0] * nrow + i2);
          alrd_v[0] += 1;
        } else if (type_refv[i] == 'c') {
          chr_v.erase(chr_v.begin() + alrd_v[1] * nrow + i2);
          alrd_v[1] += 1;
        } else if (type_refv[i] == 'b') {
          bool_v.erase(bool_v.begin() + alrd_v[2] * nrow + i2);
          alrd_v[2] += 1;
        } else if (type_refv[i] == 'i') {
          int_v.erase(int_v.begin() + alrd_v[3] * nrow + i2);
          alrd_v[3] += 1;
        } else if (type_refv[i] == 'u') {
          uint_v.erase(uint_v.begin() + alrd_v[4] * nrow + i2);
          alrd_v[4] += 1;
        } else if (type_refv[i] == 'd') {
          dbl_v.erase(dbl_v.begin() + alrd_v[5] * nrow + i2);
          alrd_v[5] += 1;
        };
      };
      nrow -= 1;
      cnt += 1;
    };
  };
};


