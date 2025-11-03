#pragma once

void transform_unique_clean(unsigned int& n) {
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
 
  unsigned int pos_vl2;
  unsigned int delta_col = nrow2 - nrow;
  for (i2 = 0; i2 < matr_idx[0].size(); i2 += 1) {
    pos_vl = matr_idx[0][i2];
    pos_vl2 = (nrow + 1) * i2;
    str_v.erase(str_v.begin() + pos_vl2, 
                    str_v.begin() + pos_vl2 + delta_col);
    tmp_val_refv[pos_vl].erase(tmp_val_refv[pos_vl].begin() + nrow, 
                    tmp_val_refv[pos_vl].end());
    tmp_val_refv[pos_vl].shrink_to_fit();
  };
  str_v.shrink_to_fit();
  for (i2 = 0; i2 < matr_idx[1].size(); i2 += 1) {
    pos_vl = matr_idx[1][i2];
    pos_vl2 = (nrow + 1) * i2;
    chr_v.erase(chr_v.begin() + pos_vl2, 
                    chr_v.begin() + pos_vl2 + delta_col);
    tmp_val_refv[pos_vl].erase(tmp_val_refv[pos_vl].begin() + nrow, 
                    tmp_val_refv[pos_vl].end());
    tmp_val_refv[pos_vl].shrink_to_fit();
  };
  chr_v.shrink_to_fit();
  for (i2 = 0; i2 < matr_idx[2].size(); i2 += 1) {
    pos_vl = matr_idx[2][i2];
    pos_vl2 = (nrow + 1) * i2;
    bool_v.erase(bool_v.begin() + pos_vl2, 
                    bool_v.begin() + pos_vl2 + delta_col);
    tmp_val_refv[pos_vl].erase(tmp_val_refv[pos_vl].begin() + nrow, 
                    tmp_val_refv[pos_vl].end());
    tmp_val_refv[pos_vl].shrink_to_fit();
  };
  bool_v.shrink_to_fit();
  for (i2 = 0; i2 < matr_idx[3].size(); i2 += 1) {
    pos_vl = matr_idx[3][i2];
    pos_vl2 = (nrow + 1) * i2;
    int_v.erase(int_v.begin() + pos_vl2, 
                    int_v.begin() + pos_vl2 + delta_col);
    tmp_val_refv[pos_vl].erase(tmp_val_refv[pos_vl].begin() + nrow, 
                    tmp_val_refv[pos_vl].end());
    tmp_val_refv[pos_vl].shrink_to_fit();
  };
  int_v.shrink_to_fit();
  for (i2 = 0; i2 < matr_idx[4].size(); i2 += 1) {
    pos_vl = matr_idx[4][i2];
    pos_vl2 = (nrow + 1) * i2;
    uint_v.erase(uint_v.begin() + pos_vl2, 
                    uint_v.begin() + pos_vl2 + delta_col);
    tmp_val_refv[pos_vl].erase(tmp_val_refv[pos_vl].begin() + nrow, 
                    tmp_val_refv[pos_vl].end());
    tmp_val_refv[pos_vl].shrink_to_fit();
  };
  uint_v.shrink_to_fit();
  for (i2 = 0; i2 < matr_idx[5].size(); i2 += 1) {
    pos_vl = matr_idx[5][i2];
    pos_vl2 = (nrow + 1) * i2;
    dbl_v.erase(dbl_v.begin() + pos_vl2, 
                    dbl_v.begin() + pos_vl2 + delta_col);
    tmp_val_refv[pos_vl].erase(tmp_val_refv[pos_vl].begin() + nrow, 
                    tmp_val_refv[pos_vl].end());
    tmp_val_refv[pos_vl].shrink_to_fit();
  };
  dbl_v.shrink_to_fit();
};


