#pragma once

std::span<const std::string> view_colstr(unsigned int &x) const {
  unsigned int i2 = 0;

  while (i2 < matr_idx[0].size()) {

    if (x == matr_idx[0][i2]) {
      break;
    };

    i2 += 1;
  };
  i2 = nrow * i2;

  return std::span<const std::string>(str_v.data() + i2, nrow);

};


