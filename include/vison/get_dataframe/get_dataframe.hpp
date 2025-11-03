#pragma once

void get_dataframe(std::vector<int> &cols, Dataframe &cur_obj) {
  unsigned int i2;
  nrow = cur_obj.get_nrow();
  const unsigned int& max_ncol = cur_obj.get_ncol();
  ncol = cols.size();
  const auto& cur_tmp = cur_obj.get_tmp_val_refv();
  std::vector<std::string> cur_v = {};
  if (cols[0] == -1) {
    cols.pop_back();
    cols.reserve(max_ncol);
    for (i2 = 0; i2 < max_ncol; ++i2) {
      cols.push_back(i2);
    };
    ncol = max_ncol;
  };
  for (int& i : cols) {
    cur_v = {};
    for (i2 = 0; i2 < nrow; ++i2) {
      cur_v.push_back(cur_tmp[i][i2]);  
    };
    tmp_val_refv.push_back(cur_v);
  };
  type_classification();
  name_v = cur_obj.get_colname();
  name_v_row = cur_obj.get_rowname(); 
};


