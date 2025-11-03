#pragma once

void transform_merge_excluding(Dataframe &obj, 
                unsigned int &key1, 
                unsigned int &key2,
                std::string default_str = "NA",
                char default_chr = ' ',
                bool default_bool = 0,
                int default_int = 0,
                unsigned int default_uint = 0,
                double default_dbl = 0) {
  const unsigned int& ncol2 = obj.get_ncol();
  std::vector<std::string> cur_vstr(nrow);
  unsigned int nrow2 = nrow;
  nrow = 0;
  unsigned int i;
  unsigned int i2;
  unsigned int i3;
  const std::vector<std::string>& name2 = obj.get_colname();
  if (name_v.size() > 0) {
    name_v.insert(name_v.end(), name2.begin(), name2.end()); 
  };
  tmp_val_refv.reserve(ncol + ncol2);
  for (i = 0; i < ncol2; ++i) {
    tmp_val_refv.push_back(cur_vstr);
  };
  const std::vector<std::vector<unsigned int>>& matr_idx2 = obj.get_matr_idx();
  const std::vector<char>& type2 = obj.get_typecol();
  type_refv.insert(type_refv.end(), type2.begin(), type2.end());
  const auto& tmp2 = obj.get_tmp_val_refv();
  const std::vector<std::string>& col1 = tmp_val_refv[key1];
  const std::vector<std::string>& col2 = tmp2[key2];
  
  //std::unordered_map<std::string, size_t> b_index; // standard map (slower)
  ankerl::unordered_dense::map<std::string, size_t> b_index;
  for (size_t j = 0; j < col2.size(); ++j) {
    b_index.insert({col2[j], j});
  };

  std::vector<std::string> str_v2 = obj.get_str_vec();
  std::vector<char> chr_v2 = obj.get_chr_vec();
  std::vector<bool> bool_v2 = obj.get_bool_vec();
  std::vector<int> int_v2 = obj.get_int_vec();
  std::vector<unsigned int> uint_v2 = obj.get_uint_vec();
  std::vector<double> dbl_v2 = obj.get_dbl_vec();

  std::vector<std::string> tmp_str_v(nrow2);
  std::vector<char> tmp_chr_v(nrow2);
  std::vector<bool> tmp_bool_v(nrow2);
  std::vector<int> tmp_int_v(nrow2);
  std::vector<unsigned int> tmp_uint_v(nrow2);
  std::vector<double> tmp_dbl_v(nrow2);

  unsigned int pos_val;

  unsigned int pre_str_val = str_v.size() / nrow2;
  unsigned int pre_chr_val = chr_v.size() / nrow2;
  unsigned int pre_bool_val = bool_v.size() / nrow2;
  unsigned int pre_int_val = int_v.size() / nrow2;
  unsigned int pre_uint_val = uint_v.size() / nrow2;
  unsigned int pre_dbl_val = dbl_v.size() / nrow2;

  for (auto& el : matr_idx[0]) {
    str_v.insert(str_v.end(), tmp_str_v.begin(), tmp_str_v.end());
  };
  for (auto& el : matr_idx[1]) {
    chr_v.insert(chr_v.end(), tmp_chr_v.begin(), tmp_chr_v.end());
  };
  for (auto& el : matr_idx[2]) {
    bool_v.insert(bool_v.end(), tmp_bool_v.begin(), tmp_bool_v.end());
  };
  for (auto& el : matr_idx[3]) {
    int_v.insert(int_v.end(), tmp_int_v.begin(), tmp_int_v.end());
  };
  for (auto& el : matr_idx[4]) {
    uint_v.insert(uint_v.end(), tmp_uint_v.begin(), tmp_uint_v.end());
  };
  for (auto& el : matr_idx[5]) {
    dbl_v.insert(dbl_v.end(), tmp_dbl_v.begin(), tmp_dbl_v.end());
  };

  std::vector<std::vector<unsigned int>> matr_idx2b = matr_idx2;
  for (auto& el : matr_idx2b) {
    for (auto& el2 : el) {
      el2 += ncol;
    };
  };

  matr_idx[0].insert(matr_idx[0].end(), 
                     matr_idx2b[0].begin(), 
                     matr_idx2b[0].end());
  matr_idx[1].insert(matr_idx[1].end(), 
                     matr_idx2b[1].begin(), 
                     matr_idx2b[1].end());
  matr_idx[2].insert(matr_idx[2].end(), 
                     matr_idx2b[2].begin(), 
                     matr_idx2b[2].end());
  matr_idx[3].insert(matr_idx[3].end(), 
                     matr_idx2b[3].begin(), 
                     matr_idx2b[3].end());
  matr_idx[4].insert(matr_idx[4].end(), 
                     matr_idx2b[4].begin(), 
                     matr_idx2b[4].end());
  matr_idx[5].insert(matr_idx[5].end(), 
                     matr_idx2b[5].begin(), 
                     matr_idx2b[5].end());

  for (size_t i = 0; i < col1.size(); ++i) { 
    auto it = b_index.find(col1[i]);

    if (it == b_index.end()) {

      for (i2 = 0; i2 < matr_idx[0].size(); i2 += 1) {
        str_v[nrow2 * i2 + nrow] = str_v[nrow2 * i2 + i];
        pos_val = matr_idx[0][i2];
        tmp_val_refv[pos_val][nrow] = tmp_val_refv[pos_val][i];
      };
      for (i2 = 0; i2 < matr_idx[1].size(); i2 += 1) {
        chr_v[nrow2 * i2 + nrow] = chr_v[nrow2 * i2 + i];
        pos_val = matr_idx[1][i2];
        tmp_val_refv[pos_val][nrow] = tmp_val_refv[pos_val][i];
      };
      for (i2 = 0; i2 < matr_idx[2].size(); i2 += 1) {
        bool_v[nrow2 * i2 + nrow] = bool_v[nrow2 * i2 + i];
        pos_val = matr_idx[2][i2];
        tmp_val_refv[pos_val][nrow] = tmp_val_refv[pos_val][i];
      };
      for (i2 = 0; i2 < matr_idx[3].size(); i2 += 1) {
        int_v[nrow2 * i2 + nrow] = int_v[nrow2 * i2 + i];
        pos_val = matr_idx[3][i2];
        tmp_val_refv[pos_val][nrow] = tmp_val_refv[pos_val][i];
      };
      for (i2 = 0; i2 < matr_idx[4].size(); i2 += 1) {
        uint_v[nrow2 * i2 + nrow] = uint_v[nrow2 * i2 + i];
        pos_val = matr_idx[4][i2];
        tmp_val_refv[pos_val][nrow] = tmp_val_refv[pos_val][i];
      };
      for (i2 = 0; i2 < matr_idx[5].size(); i2 += 1) {
        dbl_v[nrow2 * i2 + nrow] = dbl_v[nrow2 * i2 + i];
        pos_val = matr_idx[5][i2];
        tmp_val_refv[pos_val][nrow] = tmp_val_refv[pos_val][i];
      };
      for (i2 = 0; i2 < matr_idx2[0].size(); i2 += 1) {
        pos_val = pre_str_val + i2;
        str_v[nrow2 * pos_val + nrow] = default_str;
        pos_val = matr_idx2[0][i2];
        tmp_val_refv[ncol + pos_val][nrow] = default_str;
      };
      for (i2 = 0; i2 < matr_idx2[1].size(); i2 += 1) {
        pos_val = pre_chr_val + i2;
        chr_v[nrow2 * pos_val + nrow] = default_chr;
        pos_val = matr_idx2[1][i2];
        tmp_val_refv[ncol + pos_val][nrow] = " ";
      };
      for (i2 = 0; i2 < matr_idx2[2].size(); i2 += 1) {
        pos_val = pre_bool_val + i2;
        bool_v[nrow2 * pos_val + nrow] = default_bool;
        pos_val = matr_idx2[2][i2];
        tmp_val_refv[ncol + pos_val][nrow] = std::to_string(default_bool);
      };
      for (i2 = 0; i2 < matr_idx2[3].size(); i2 += 1) {
        pos_val = pre_int_val + i2;
        int_v[nrow2 * pos_val + nrow] = default_int;
        pos_val = matr_idx2[3][i2];
        tmp_val_refv[ncol + pos_val][nrow] = std::to_string(default_int);
      };
      for (i2 = 0; i2 < matr_idx2[4].size(); i2 += 1) {
        pos_val = pre_uint_val + i2;
        uint_v[nrow2 * pos_val + nrow] = default_uint;
        pos_val = matr_idx2[4][i2];
        tmp_val_refv[ncol + pos_val][nrow] = std::to_string(default_uint);
      };
      for (i2 = 0; i2 < matr_idx2[5].size(); i2 += 1) {
        pos_val = pre_dbl_val + i2;
        dbl_v[nrow2 * pos_val + nrow] = default_dbl;
        pos_val = matr_idx2[5][i2];
        tmp_val_refv[ncol + pos_val][nrow] = std::to_string(default_dbl);
      };
      nrow += 1;
    };
  };
  ncol += ncol2;
};


