#pragma once

void transform_unique(unsigned int& n) {
  unsigned int nrow2 = nrow;
  unsigned int i;
  unsigned int i2;
  unsigned int pos_vl;
  nrow = 0;
  //std::unordered_set<std::string> unic_v; // standard set (slower)
  ankerl::unordered_dense::set<std::string> unic_v;

  for (i = 0; i < nrow2; i += 1) {
    if (!unic_v.contains(tmp_val_refv[n][i])) {
      unic_v.insert(tmp_val_refv[n][i]);
      for (i2 = 0; i2 < matr_idx[0].size(); i2 += 1) {
        pos_vl = matr_idx[0][i2];
        tmp_val_refv[pos_vl][nrow] = tmp_val_refv[pos_vl][i];
        str_v[nrow2 * i2 + nrow] = str_v[nrow2 * i2 + i];
      };
      for (i2 = 0; i2 < matr_idx[1].size(); i2 += 1) {
        pos_vl = matr_idx[1][i2];
        tmp_val_refv[pos_vl][nrow] = tmp_val_refv[pos_vl][i];
        chr_v[nrow2 * i2 + nrow] = chr_v[nrow2 * i2 + i];
      };
      for (i2 = 0; i2 < matr_idx[2].size(); i2 += 1) {
        pos_vl = matr_idx[2][i2];
        tmp_val_refv[pos_vl][nrow] = tmp_val_refv[pos_vl][i];
        bool_v[nrow2 * i2 + nrow] = bool_v[nrow2 * i2 + i];
      };
      for (i2 = 0; i2 < matr_idx[3].size(); i2 += 1) {
        pos_vl = matr_idx[3][i2];
        tmp_val_refv[pos_vl][nrow] = tmp_val_refv[pos_vl][i];
        int_v[nrow2 * i2 + nrow] = int_v[nrow2 * i2 + i];
      };
      for (i2 = 0; i2 < matr_idx[4].size(); i2 += 1) {
        pos_vl = matr_idx[4][i2];
        tmp_val_refv[pos_vl][nrow] = tmp_val_refv[pos_vl][i];
        uint_v[nrow2 * i2 + nrow] = uint_v[nrow2 * i2 + i];
      };
      for (i2 = 0; i2 < matr_idx[5].size(); i2 += 1) {
        pos_vl = matr_idx[5][i2];
        tmp_val_refv[pos_vl][nrow] = tmp_val_refv[pos_vl][i];
        dbl_v[nrow2 * i2 + nrow] = dbl_v[nrow2 * i2 + i];
      };
      nrow += 1;
    };
  };
};


