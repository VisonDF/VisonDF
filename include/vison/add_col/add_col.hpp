#pragma once

template <typename T> void add_col(std::vector<T> &x, std::string name = "NA") {
  if (x.size() != nrow) {
    std::cerr << "Error: vector length (" << x.size()
              << ") does not match nrow (" << nrow << ")\n";
    return;
  };
  name_v.push_back(name);
  std::vector<std::string> cur_v = {};
  cur_v.reserve(nrow);
  if constexpr (std::is_same_v<T, bool>) {
    matr_idx[2].push_back(ncol);
    type_refv.push_back('b');
    for (auto &i : x) {
      bool_v.push_back(i);
      cur_v.push_back(std::to_string(i));
    };
  } else if constexpr (std::is_same_v<T, IntT>) {
    matr_idx[3].push_back(ncol);
    type_refv.push_back('i');
    for (auto &i : x) {
      int_v.push_back(i);
      cur_v.push_back(std::to_string(i));
    };
  } else if constexpr (std::is_same_v<T, UIntT>) {
    matr_idx[4].push_back(ncol);
    type_refv.push_back('u');
    for (auto &i : x) {
      uint_v.push_back(i);
      cur_v.push_back(std::to_string(i));
    };
  } else if constexpr (std::is_same_v<T, FloatT>) {
    matr_idx[5].push_back(ncol);
    type_refv.push_back('d');
    for (auto &i : x) {
      dbl_v.push_back(i);
      cur_v.push_back(std::to_string(i));
    }; 
  } else if constexpr (std::is_same_v<T, char>) {
    matr_idx[1].push_back(ncol);
    type_refv.push_back('c');
    for (auto &i : x) {
      chr_v.push_back(i);
      cur_v.push_back(std::string(1, i));
    }; 
  } else if constexpr (std::is_same_v<T, std::string>) {
    matr_idx[0].push_back(ncol);
    type_refv.push_back('s');
    for (auto &i : x) {
      str_v.push_back(i);
      cur_v.push_back(i);
    }; 
  } else {
    std::cerr << "Error in (add_col) type not suported \n";
  };
  tmp_val_refv.push_back(cur_v);
  ncol += 1;
};


