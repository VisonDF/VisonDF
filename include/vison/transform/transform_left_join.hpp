#pragma once

void transform_left_join(Dataframe &obj, 
                unsigned int &key1, 
                unsigned int &key2,
                std::string default_str = "NA",
                char default_chr = ' ',
                bool default_bool = 0,
                int default_int = 0,
                unsigned int default_uint = 0,
                double default_dbl = 0) {
  const unsigned int& ncol2 = obj.get_ncol();
  const std::vector<std::vector<unsigned int>>& matr_idx2 = obj.get_matr_idx();

  const std::vector<std::string>& str_v2 = obj.get_str_vec();
  const std::vector<char>& chr_v2 = obj.get_chr_vec();
  const std::vector<bool>& bool_v2 = obj.get_bool_vec();
  const std::vector<int>& int_v2 = obj.get_int_vec();
  const std::vector<unsigned int>& uint_v2 = obj.get_uint_vec();
  const std::vector<double>& dbl_v2 = obj.get_dbl_vec();
  
  const unsigned int size_str = str_v.size() / nrow;
  const unsigned int size_chr = chr_v.size() / nrow;
  const unsigned int size_bool = bool_v.size() / nrow;
  const unsigned int size_int = int_v.size() / nrow;
  const unsigned int size_uint = uint_v.size() / nrow;
  const unsigned int size_dbl = dbl_v.size() / nrow;
  
  unsigned int i;
  unsigned int i2;
  
  std::vector<std::string> vec_str(nrow);

  for (i = 0; i < ncol2; i += 1) {
    tmp_val_refv.push_back(vec_str);
  };

  const std::vector<char>& vec_type = obj.get_typecol();
  const std::vector<std::string>& col1 = tmp_val_refv[key1];
  const std::vector<std::vector<std::string>>& tmp_val_refv2 = obj.get_tmp_val_refv();
  const std::vector<std::string>& col2 = tmp_val_refv2[key2];

  str_v.resize(str_v.size() + nrow * matr_idx2[0].size());
  chr_v.resize(chr_v.size() + nrow * matr_idx2[1].size());
  bool_v.resize(bool_v.size() + nrow * matr_idx2[2].size());
  int_v.resize(int_v.size() + nrow * matr_idx2[3].size());
  uint_v.resize(uint_v.size() + nrow * matr_idx2[4].size());
  dbl_v.resize(dbl_v.size() + nrow * matr_idx2[5].size());
 
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


  std::unordered_multimap<std::string, size_t> lookup;
  for (i = 0; i < col2.size(); i += 1) {
    lookup.insert({col2[i], i});
  };

  unsigned int idx;
  unsigned int pos_vl;
  const unsigned int& nrow2 = obj.get_nrow();
  for (i = 0; i < col1.size(); i += 1) {

    auto it = lookup.find(col1[i]);
    if (it == lookup.end()) {
      for (i2 = 0; i2 < matr_idx2[0].size(); i2 += 1) {
        tmp_val_refv[ncol + matr_idx2[0][i2]][i] = default_str;
        str_v[nrow * (size_str + i2) + i] = default_str;
      };
      for (i2 = 0; i2 < matr_idx2[1].size(); i2 += 1) {
        tmp_val_refv[ncol + matr_idx2[1][i2]][i] = std::string(1, default_chr);
        chr_v[nrow * (size_chr + i2) + i] = default_chr;
      };
      for (i2 = 0; i2 < matr_idx2[2].size(); i2 += 1) {
        tmp_val_refv[ncol + matr_idx2[2][i2]][i] = std::to_string(default_bool);
        bool_v[nrow * (size_bool + i2) + i] = default_bool;
      };
      for (i2 = 0; i2 < matr_idx2[3].size(); i2 += 1) {
        tmp_val_refv[ncol + matr_idx2[3][i2]][i] = std::to_string(default_int);
        int_v[nrow * (size_int + i2) + i] = default_int;
      };
      for (i2 = 0; i2 < matr_idx2[4].size(); i2 += 1) {
        tmp_val_refv[ncol + matr_idx2[4][i2]][i] = std::to_string(default_uint);
        uint_v[nrow * (size_uint + i2) + i] = default_uint;
      };
      for (i2 = 0; i2 < matr_idx2[5].size(); i2 += 1) {
        tmp_val_refv[ncol + matr_idx2[5][i2]][i] = std::to_string(default_dbl);
        dbl_v[nrow * (size_dbl + i2) + i] = default_dbl;
      };
    } else {
      idx = it->second;
      for (i2 = 0; i2 < matr_idx2[0].size(); i2 += 1) {
        str_v[nrow * (size_str + i2) + i] = str_v2[nrow2 * i2 + idx];
        pos_vl = matr_idx2[0][i2];
        tmp_val_refv[ncol + pos_vl][i] = tmp_val_refv2[pos_vl][idx];
      };
      for (i2 = 0; i2 < matr_idx2[1].size(); i2 += 1) {
        chr_v[nrow * (size_chr + i2) + i] = chr_v2[nrow2 * i2 + idx];
        pos_vl = matr_idx2[1][i2];
        tmp_val_refv[ncol + pos_vl][i] = tmp_val_refv2[pos_vl][idx];
      };
      for (i2 = 0; i2 < matr_idx2[2].size(); i2 += 1) {
        bool_v[nrow * (size_bool + i2) + i] = bool_v2[nrow2 * i2 + idx];
        pos_vl = matr_idx2[2][i2];
        tmp_val_refv[ncol + pos_vl][i] = tmp_val_refv2[pos_vl][idx];
      };
      for (i2 = 0; i2 < matr_idx2[3].size(); i2 += 1) {
        int_v[nrow * (size_int + i2) + i] = int_v2[nrow2 * i2 + idx];
        pos_vl = matr_idx2[3][i2];
        tmp_val_refv[ncol + pos_vl][i] = tmp_val_refv2[pos_vl][idx];
      };
      for (i2 = 0; i2 < matr_idx2[4].size(); i2 += 1) {
        uint_v[nrow * (size_uint + i2) + i] = uint_v2[nrow2 * i2 + idx];
        pos_vl = matr_idx2[4][i2];
        tmp_val_refv[ncol + pos_vl][i] = tmp_val_refv2[pos_vl][idx];
      };
      for (i2 = 0; i2 < matr_idx2[5].size(); i2 += 1) {
        dbl_v[nrow * (size_dbl + i2) + i] = dbl_v2[nrow2 * i2 + idx];
        pos_vl = matr_idx2[5][i2];
        tmp_val_refv[ncol + pos_vl][i] = tmp_val_refv2[pos_vl][idx];
      };
    };
  };
  type_refv.insert(type_refv.end(), vec_type.begin(), vec_type.end());
  ncol += ncol2;
  const std::vector<std::string>& colname2 = obj.get_colname();
  if (colname2.size() > 0) {
    name_v.insert(name_v.end(), colname2.begin(), colname2.end());
  } else {
    name_v.resize(ncol);
  };
};


