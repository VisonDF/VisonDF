#pragma once

void merge_all2(Dataframe &obj1, 
                Dataframe &obj2, 
                bool colname, 
                unsigned int &key1, 
                unsigned int &key2,
                std::string default_str = "NA",
                std::string default_chr = " ",
                std::string default_bool = "0",
                std::string default_int = "0",
                std::string default_uint = "0",
                std::string default_dbl = "0") {
  const unsigned int& ncol1 = obj1.get_ncol();
  const unsigned int& ncol2 = obj2.get_ncol();
  std::vector<std::string> cur_vstr;
  ncol = ncol1 + ncol2;
  unsigned int i;
  unsigned int i2;
  unsigned int i3;
  const std::vector<std::vector<unsigned int>>& matr_idx1 = obj1.get_matr_idx();
  const std::vector<std::vector<unsigned int>>& matr_idx2 = obj2.get_matr_idx();
  const std::vector<std::string>& name1 = obj1.get_colname();
  const std::vector<std::string>& name2 = obj2.get_colname();
  if (colname) {
    name_v.resize(ncol);
    if (name1.size() > 0) {
      for (i = 0; i < name1.size(); ++i) {
        name_v.push_back(name1[i]);
      };
    };
    if (name2.size() > 0) {
      for (i = 0; i < name2.size(); ++i) {
        name_v.push_back(name2[i]);
      };
    };
  };
  tmp_val_refv.reserve(ncol);
  for (i = 0; i < ncol; ++i) {
    tmp_val_refv.push_back(cur_vstr);
  };
  const std::vector<char>& type1 = obj1.get_typecol();
  const std::vector<char>& type2 = obj2.get_typecol();
  const auto& tmp1 = obj1.get_tmp_val_refv();
  const auto& tmp2 = obj2.get_tmp_val_refv();
  const std::vector<std::string>& col1 = tmp1[key1];
  const std::vector<std::string>& col2 = tmp2[key2];
  std::unordered_multimap<std::string, size_t> b_index;
  for (size_t j = 0; j < col2.size(); ++j) {
    b_index.insert({col2[j], j});
  };
  std::unordered_multimap<std::string, size_t> a_index;
  for (size_t j = 0; j < col1.size(); ++j) {
    a_index.insert({col1[j], j});
  };
  for (size_t i = 0; i < col1.size(); ++i) {
    auto it = b_index.find(col1[i]);
    if (it == b_index.end()) {
      nrow += 1;
      for (i2 = 0; i2 < ncol1; i2 += 1) {
        tmp_val_refv[i2].push_back(tmp1[i2][i]);
      };
      for (auto& el : matr_idx2[0]) {
        tmp_val_refv[ncol1 + el].push_back(default_str);
      };
      for (auto& el : matr_idx2[1]) {
        tmp_val_refv[ncol1 + el].push_back(default_chr);
      };
      for (auto& el : matr_idx2[2]) {
        tmp_val_refv[ncol1 + el].push_back(default_bool);
      };
      for (auto& el : matr_idx2[3]) {
        tmp_val_refv[ncol1 + el].push_back(default_int);
      };
      for (auto& el : matr_idx2[4]) {
        tmp_val_refv[ncol1 + el].push_back(default_uint);
      };
      for (auto& el : matr_idx2[5]) {
        tmp_val_refv[ncol1 + el].push_back(default_dbl);
      };
    } else {
      nrow += 1;
      size_t idx = it->second;
      for (i2 = 0; i2 < ncol1; i2 += 1) {
        tmp_val_refv[i2].push_back(tmp1[i2][i]);
      };
      for (i2 = 0; i2 < ncol2; i2 += 1) {
        tmp_val_refv[ncol1 + i2].push_back(tmp2[i2][idx]);
      };
    };
  };
  for (size_t i = 0; i < col2.size(); ++i) {
    auto it = a_index.find(col2[i]);
    if (it == a_index.end()) {
      nrow += 1;
      for (i2 = 0; i2 < ncol2; i2 += 1) {
        tmp_val_refv[ncol1 + i2].push_back(tmp2[i2][i]);
      };
      for (auto& el : matr_idx1[0]) {
        tmp_val_refv[el].push_back(default_str);
      };
      for (auto& el : matr_idx1[1]) {
        tmp_val_refv[el].push_back(default_chr);
      };
      for (auto& el : matr_idx1[2]) {
        tmp_val_refv[el].push_back(default_bool);
      };
      for (auto& el : matr_idx1[3]) {
        tmp_val_refv[el].push_back(default_int);
      };
      for (auto& el : matr_idx1[4]) {
        tmp_val_refv[el].push_back(default_uint);
      };
      for (auto& el : matr_idx1[5]) {
        tmp_val_refv[el].push_back(default_dbl);
      };
      for (i2 = 0; i2 < ncol2; i2 += 1) {
        tmp_val_refv[i2].push_back(tmp2[i2][i]);
      };
    };
  };
  type_classification();
};


