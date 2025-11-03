#pragma once

std::span<const UIntT> view_coluint(unsigned int &x) const {
  unsigned int i2 = 0;

  while (i2 < matr_idx[4].size()) {

    if (x == matr_idx[4][i2]) {
      break;
    };

    i2 += 1;
  };
  i2 = nrow * i2;

  return std::span<const UIntT>(uint_v.data() + i2, nrow);

};
