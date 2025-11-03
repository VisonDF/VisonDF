#pragma once

std::span<const char> view_colchr(unsigned int &x) const {
  unsigned int i2 = 0;

  while (i2 < matr_idx[1].size()) {

    if (x == matr_idx[1][i2]) {
      break;
    };

    i2 += 1;
  };
  i2 = nrow * i2;

  return std::span<const char>(chr_v.data() + i2, nrow);

};


