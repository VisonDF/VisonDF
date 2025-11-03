#pragma once

void get_dataframe(std::vector<int> &cols, 
                Dataframe &cur_obj, 
                std::vector<bool>& mask) {
  unsigned int i2;
  nrow = std::count(mask.begin(), mask.end(), true);
  const unsigned int& max_ncol = cur_obj.get_ncol();
  ncol = cols.size();
  const auto& cur_tmp = cur_obj.get_tmp_val_refv();
  std::vector<std::string> cur_v;
  cur_v.reserve(nrow);
  const std::vector<std::string>& name_v_row1 = cur_obj.get_rowname();
  const std::vector<std::string>& name_v1 = cur_obj.get_colname();
  const std::vector<char>& type_refv1 = cur_obj.get_typecol();
  if (cols[0] == -1) {
    type_refv.resize(ncol);
    name_v.resize(ncol);
    cols.pop_back();
    cols.reserve(max_ncol);
    for (i2 = 0; i2 < max_ncol; ++i2) {
      cols.push_back(i2);
      name_v[i2] = name_v1[i2];
      type_refv[i2] = type_refv1[i2];
    };
    ncol = max_ncol;
  };
  for (int& i : cols) {
    cur_v.resize(nrow);
    for (i2 = 0; i2 < nrow; ++i2) {
      if (mask[i2]) {
        cur_v[i2] = cur_tmp[i][i2];
      }
    };
    tmp_val_refv.emplace_back(); 
    tmp_val_refv.back().swap(cur_v);
    cur_v.reserve(nrow);
  };
  if (!name_v_row1.empty()) {
    size_t i2 = 0;
    name_v_row.resize(nrow);
    for (size_t i = 0; i <  mask.size(); i += 1) {
      if (mask[i]) {
        name_v_row[i2] = name_v_row1[i];
        i2 += 1;
      }
    };
  }
};




