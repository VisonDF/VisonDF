#pragma once

void concat(Dataframe& obj) {
  const unsigned int& ncol2 = obj.get_ncol();
  if (ncol != ncol2) {
    std::cerr << "Can't concatenate 2 dataframes with different number of columns\n";
    return;
  };
  const std::vector<char> type_refv2 = obj.get_typecol();
  if (type_refv != type_refv2) {
    std::cerr << "Can't concatenate 2 dataframes with different column type\n";
    return;
  };

  const std::vector<std::string>& str_v2 = obj.get_str_vec();
  const std::vector<char>& chr_v2 = obj.get_chr_vec();
  const std::vector<bool>& bool_v2 = obj.get_bool_vec();
  const std::vector<int>& int_v2 = obj.get_int_vec();
  const std::vector<unsigned int>& uint_v2 = obj.get_uint_vec();
  const std::vector<double>& dbl_v2 = obj.get_dbl_vec();

  const std::vector<std::vector<std::string>>& tmp_val_refv2 = obj.get_tmp_val_refv();

  size_t i2 = 0;
  size_t i = 0;

  const unsigned int& nrow2 = obj.get_nrow();
  unsigned int pre_nrow = nrow;
  nrow += nrow2;

  str_v.reserve(str_v.size() + str_v2.size());
  for (auto& el : matr_idx[0]) {
    tmp_val_refv[el].reserve(nrow);
    tmp_val_refv[el].insert(tmp_val_refv[el].end(), 
                    tmp_val_refv2[el].begin(),
                    tmp_val_refv2[el].end());
    str_v.insert(str_v.begin() + (i2 + 1) * pre_nrow, 
                    str_v2.begin() + i * nrow2,
                    str_v2.begin() + (i + 1) * nrow2);
    i2 += 2;
    i += 1;
  };

  i2 = 0;
  i = 0;
  chr_v.reserve(chr_v.size() + chr_v2.size());

  for (auto& el : matr_idx[1]) {
    tmp_val_refv[el].reserve(nrow);
    tmp_val_refv[el].insert(tmp_val_refv[el].end(), 
                    tmp_val_refv2[el].begin(),
                    tmp_val_refv2[el].end());
    chr_v.insert(chr_v.begin() + (i2 + 1) * pre_nrow, 
                    chr_v2.begin() + i * nrow2,
                    chr_v2.begin() + (i + 1) * nrow2);
    i2 += 2;
    i += 1;
  };

  i2 = 0;
  i = 0;
  bool_v.reserve(bool_v.size() + bool_v2.size());

  for (auto& el : matr_idx[2]) {
    tmp_val_refv[el].reserve(nrow);
    tmp_val_refv[el].insert(tmp_val_refv[el].end(), 
                    tmp_val_refv2[el].begin(),
                    tmp_val_refv2[el].end());
    bool_v.insert(bool_v.begin() + (i2 + 1) * pre_nrow, 
                    bool_v2.begin() + i * nrow2,
                    bool_v2.begin() + (i + 1) * nrow2);
    i2 += 2;
    i += 1;
  };

  i2 = 0;
  i = 0;
  int_v.reserve(int_v.size() + int_v2.size());

  for (auto& el : matr_idx[3]) {
    tmp_val_refv[el].reserve(nrow);
    tmp_val_refv[el].insert(tmp_val_refv[el].end(), 
                    tmp_val_refv2[el].begin(),
                    tmp_val_refv2[el].end());
    int_v.insert(int_v.begin() + (i2 + 1) * pre_nrow, 
                    int_v2.begin() + i * nrow2,
                    int_v2.begin() + (i + 1) * nrow2);
    i2 += 2;
    i += 1;
  };

  i2 = 0;
  i = 0;
  uint_v.reserve(uint_v.size() + uint_v2.size());
  
  for (auto& el : matr_idx[4]) {
    tmp_val_refv[el].reserve(nrow);
    tmp_val_refv[el].insert(tmp_val_refv[el].end(), 
                    tmp_val_refv2[el].begin(),
                    tmp_val_refv2[el].end());
    uint_v.insert(uint_v.begin() + (i2 + 1) * pre_nrow, 
                    uint_v2.begin() + i * nrow2,
                    uint_v2.begin() + (i + 1) * nrow2);
    i2 += 2;
    i += 1;
  };

  i2 = 0;
  i = 0;
  dbl_v.reserve(dbl_v.size() + dbl_v2.size());
  
  for (auto& el : matr_idx[5]) {
    tmp_val_refv[el].reserve(nrow);
    tmp_val_refv[el].insert(tmp_val_refv[el].end(), 
                    tmp_val_refv2[el].begin(),
                    tmp_val_refv2[el].end());
    dbl_v.insert(dbl_v.begin() + (i2 + 1) * pre_nrow, 
                    dbl_v2.begin() + i * nrow2,
                    dbl_v2.begin() + (i + 1) * nrow2);
    i2 += 2;
    i += 1;
  };

};


