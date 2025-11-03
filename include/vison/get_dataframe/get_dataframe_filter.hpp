#pragma once

void get_dataframe_filter(std::vector<int> &cols, 
                Dataframe &cur_obj,
                std::vector<bool> &mask) {
  unsigned int i2;
  const unsigned int& max_ncol = cur_obj.get_ncol();
  ncol = cols.size();
  const auto& cur_tmp = cur_obj.get_tmp_val_refv();
  if (cols[0] == -1) {
    cols.pop_back();
    cols.reserve(max_ncol);
    for (i2 = 0; i2 < max_ncol; ++i2) {
      cols.push_back(i2);
    };
    ncol = max_ncol;
  };
  nrow = count(mask.begin(), mask.end(), true);
  std::vector<std::string> cur_v(nrow);
  tmp_val_refv.resize(cols.size(), cur_v);
  unsigned int i3;
  for (int& i : cols) {
    i3 = 0;
    for (i2 = 0; i2 < mask.size(); ++i2) {
      if (mask[i2]) {
        tmp_val_refv[i][i3] = cur_tmp[i][i2];
        i3 += 1;
      };
    };
  };
  type_classification();
  name_v = cur_obj.get_colname();
  name_v_row = cur_obj.get_rowname(); 
};


