#pragma once

void transform_excluding(Dataframe &cur_obj, unsigned int &in_col, unsigned int &ext_col) {
  unsigned int i2;
  unsigned int nrow2 = nrow;
  nrow = 0;
  std::vector<std::vector<std::string>> cur_tmp = cur_obj.get_tmp_val_refv();
  const std::vector<std::string>& ext_colv = cur_tmp[ext_col];
  const std::vector<std::string>& in_colv = tmp_val_refv[in_col];
  std::string cur_val;
  unsigned int pos_vl;
  const unsigned int& ext_nrow = cur_obj.get_nrow();

  //std::unordered_set<std::string> lookup; // standard set (slower)
  ankerl::unordered_dense::set<std::string> lookup; 

  lookup.reserve(ext_nrow);
  for (int j = 0; j < ext_nrow; j += 1) {
    lookup.insert(ext_colv[j]);
  };

  for (int i = 0; i < nrow2; i += 1) {
    if (!(lookup.contains(in_colv[i]))) {
      for (i2 = 0; i2 < matr_idx[0].size(); i2 += 1) {
        pos_vl = matr_idx[0][i2];
        str_v[i2 * nrow2 + nrow] = str_v[i2 * nrow2 + i];
        tmp_val_refv[pos_vl][nrow] = tmp_val_refv[pos_vl][i];
      };
      for (i2 = 0; i2 < matr_idx[1].size(); i2 += 1) {
        pos_vl = matr_idx[1][i2];
        chr_v[i2 * nrow2 + nrow] = chr_v[i2 * nrow2 + i];
        tmp_val_refv[pos_vl][nrow] = tmp_val_refv[pos_vl][i];
      };
      for (i2 = 0; i2 < matr_idx[2].size(); i2 += 1) {
        pos_vl = matr_idx[2][i2];
        bool_v[i2 * nrow2 + nrow] = bool_v[i2 * nrow2 + i];
        tmp_val_refv[pos_vl][nrow] = tmp_val_refv[pos_vl][i];
      };
      for (i2 = 0; i2 < matr_idx[3].size(); i2 += 1) {
        pos_vl = matr_idx[3][i2];
        int_v[i2 * nrow2 + nrow] = int_v[i2 * nrow2 + i];
        tmp_val_refv[pos_vl][nrow] = tmp_val_refv[pos_vl][i];
      };
      for (i2 = 0; i2 < matr_idx[4].size(); i2 += 1) {
        pos_vl = matr_idx[4][i2];
        uint_v[i2 * nrow2 + nrow] = uint_v[i2 * nrow2 + i];
        tmp_val_refv[pos_vl][nrow] = tmp_val_refv[pos_vl][i];
      };
      for (i2 = 0; i2 < matr_idx[5].size(); i2 += 1) {
        pos_vl = matr_idx[5][i2];
        dbl_v[i2 * nrow2 + nrow] = dbl_v[i2 * nrow2 + i];
        tmp_val_refv[pos_vl][nrow] = tmp_val_refv[pos_vl][i];
      };

      nrow += 1;
    };
  };
};


