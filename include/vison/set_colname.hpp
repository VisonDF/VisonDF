#pragma once

void set_colname(std::vector<std::string> &x) {
  if (x.size() != ncol) {
    std::cout << "the number of columns of the dataframe does not correspond to the size of the input column name vector";
    return;
  } else {
    name_v = x;
  };
};
