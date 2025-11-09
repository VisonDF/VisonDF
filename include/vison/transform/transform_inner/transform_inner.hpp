#pragma once

void transform_inner(Dataframe &cur_obj, unsigned int &in_col, unsigned int &ext_col) {
  unsigned int i2;
  const auto& cur_tmp = cur_obj.get_tmp_val_refv();
  const std::vector<std::string>& ext_colv = cur_tmp[ext_col];
  const std::vector<std::string>& in_colv = tmp_val_refv[in_col];
  std::string cur_val;
  unsigned int nrow2 = nrow;
  unsigned int pos_colv;
  nrow = 0;
  const unsigned int& ext_nrow = cur_obj.get_nrow();

  //std::unordered_set<std::string> lookup // standard set (slower);
  ankerl::unordered_dense::set<std::string> lookup;

  lookup.reserve(ext_nrow);
  for (unsigned int j = 0; j < ext_nrow; ++j)
    lookup.insert(ext_colv[j]);

  for (int i = 0; i < nrow2; ++i) {
    if (lookup.contains(in_colv[i])) {
      for (i2 = 0; i2 < matr_idx[0].size(); i2 += 1) {
        pos_colv = matr_idx[0][i2];
        str_v[nrow2 * i2 + nrow] = str_v[nrow2 * i2 + i];
        tmp_val_refv[pos_colv][nrow] = tmp_val_refv[pos_colv][i];
      };
      for (i2 = 0; i2 < matr_idx[1].size(); i2 += 1) {
        pos_colv = matr_idx[1][i2];
        chr_v[nrow2 * i2 + nrow] = chr_v[nrow2 * i2 + i];
        tmp_val_refv[pos_colv][nrow] = tmp_val_refv[pos_colv][i];
      };
      for (i2 = 0; i2 < matr_idx[2].size(); i2 += 1) {
        pos_colv = matr_idx[2][i2];
        bool_v[nrow2 * i2 + nrow] = bool_v[nrow2 * i2 + i];
        tmp_val_refv[pos_colv][nrow] = tmp_val_refv[pos_colv][i];
      };
      for (i2 = 0; i2 < matr_idx[3].size(); i2 += 1) {
        pos_colv = matr_idx[3][i2];
        int_v[nrow2 * i2 + nrow] = int_v[nrow2 * i2 + i];
        tmp_val_refv[pos_colv][nrow] = tmp_val_refv[pos_colv][i];
      };
      for (i2 = 0; i2 < matr_idx[4].size(); i2 += 1) {
        pos_colv = matr_idx[4][i2];
        uint_v[nrow2 * i2 + nrow] = uint_v[nrow2 * i2 + i];
        tmp_val_refv[pos_colv][nrow] = tmp_val_refv[pos_colv][i];
      };
      for (i2 = 0; i2 < matr_idx[5].size(); i2 += 1) {
        pos_colv = matr_idx[5][i2];
        dbl_v[nrow2 * i2 + nrow] = dbl_v[nrow2 * i2 + i];
        tmp_val_refv[pos_colv][nrow] = tmp_val_refv[pos_colv][i];
      };
      nrow += 1;
    };
  };
};


