#pragma once

std::vector<const UIntT>& view_coluint(unsigned int x) const {
  const auto& pos_idx = matr_idx[4];
  for (size_t i2 = 0; i2 < pos_idx.size(); ++i2) {
      if (x == pos_idx[i2]) {
          return uint_v[i2];
      };
  };
};


