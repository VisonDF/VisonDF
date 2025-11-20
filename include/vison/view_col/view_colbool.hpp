#pragma once

std::span<const uint8_t> view_colbool(unsigned int &x) const {
  unsigned int i2 = 0;

  while (i2 < matr_idx[2].size()) {

    if (x == matr_idx[2][i2]) {
      break;
    };

    i2 += 1;
  };
  i2 = nrow * i2;

  return std::span<const uint8_t>(bool_v.data() + i2, nrow);

};


