#pragma once

void get_dataframe_filter_idx(std::vector<int> &cols, 
                Dataframe &cur_obj,
                std::vector<int> &mask) {
  unsigned int i2;
  const unsigned int& max_ncol = cur_obj.get_ncol();
  const unsigned int& max_nrow = cur_obj.get_nrow();
  ncol = cols.size();
  const auto& cur_tmp = cur_obj.get_tmp_val_refv();
  if (mask[0] == -1) {
    mask.pop_back();
    mask.reserve(max_nrow);
    for (i2 = 0; i2 < max_nrow; i2 += 1) {
      mask.push_back(i2);
    };
  };
  nrow = mask.size();
  std::vector<std::string> cur_v(mask.size());
  if (cols[0] == -1) {
    cols.pop_back();
    cols.reserve(max_ncol);
    for (i2 = 0; i2 < max_ncol; ++i2) {
      cols.push_back(i2);
    };
    ncol = max_ncol;
  };
  tmp_val_refv.resize(cols.size(), cur_v);
  for (int& i : cols) {
    i2 = 0;
    for (const auto& el : mask) {
      tmp_val_refv[i][i2] = cur_tmp[i][el];
      i2 += 1;
    };
  };
  type_classification();
  name_v = cur_obj.get_colname();
  name_v_row = cur_obj.get_rowname(); 
};


