#pragma once

std::span<const IntT> view_colint(unsigned int &x) const {
  unsigned int i2 = 0;

  while (i2 < matr_idx[3].size()) {

    if (x == matr_idx[3][i2]) {
      break;
    };

    i2 += 1;
  };
  i2 = nrow * i2;

  return std::span<const IntT>(int_v.data() + i2, nrow);

};


