#pragma once

void longest_determine() {
  longest_v = {};
  unsigned int i;
  unsigned int i2;
  if (name_v.size() > 0) {
    for (i = 0; i < ncol; ++i) {
      longest_v.push_back(name_v[i].length());
    };
  } else {
    longest_v.resize(ncol, 0);
  }; 
  for (i = 0; i < ncol; ++i) {
    for (i2 = 0; i2 < nrow; ++i2) {
      if (tmp_val_refv[i][i2].length() > longest_v[i]) {
        longest_v[i] = tmp_val_refv[i][i2].length();

      };
    };
  };
};
