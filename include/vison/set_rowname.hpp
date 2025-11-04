#pragma once

void set_rowname(std::vector<std::string> &x) {
  if (x.size() != nrow) {
    std::cout << "the number of columns of the dataframe does not correspond to the size of the input column name vector";
    return;
  } else {
    name_v_row = x;
  };
};


