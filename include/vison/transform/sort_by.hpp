#pragma once

template <bool asc = 1>
void sort_by(unsigned int& n) {

  std::vector<size_t> idx(nrow);
  std::iota(idx.begin(), idx.end(), 0);
  unsigned int i = 0;
  unsigned int i2;
  if constexpr (asc) {
    switch(type_refv[n]) {
      case 's': {

                  while (n != matr_idx[0][i]) {
                    i += 1;
                  }
                  if (i == matr_idx[0].size()) {
                    std::cerr << "Column not found.\n";
                    return;
                  };
                  const std::span<std::string> values(str_v.data() + i * nrow, nrow);
                  std::sort(idx.begin(), idx.end(),
                  [&](size_t a, size_t b) {
                      return values[a] < values[b];
                  });

                  break;

                };
      case 'c': {

                  while (n != matr_idx[1][i]) {
                    i += 1;
                  }
                  if (i == matr_idx[1].size()) {
                    std::cerr << "Column not found.\n";
                    return;
                  };
                  const std::span<char> values(chr_v.data() + i * nrow, nrow);
                  std::sort(idx.begin(), idx.end(),
                  [&](size_t a, size_t b) {
                      return values[a] < values[b];
                  });

                  break;

                };
      case 'b': {

                  while (n != matr_idx[2][i]) {
                    i += 1;
                  }
                  if (i == matr_idx[2].size()) {
                    std::cerr << "Column not found.\n";
                    return;
                  };
                  std::vector<bool> values(bool_v.begin() + i * nrow, 
                                  bool_v.begin() + (i + 1) * nrow);
                  std::sort(idx.begin(), idx.end(),
                  [&](size_t a, size_t b) {
                      return values[a] < values[b];
                  });

                  break;

                };
      case 'i': {

                  while (n != matr_idx[3][i]) {
                    i += 1;
                  }
                  if (i == matr_idx[3].size()) {
                    std::cerr << "Column not found.\n";
                    return;
                  };
                  const std::span<int> values(int_v.data() + i * nrow, nrow);
                  std::sort(idx.begin(), idx.end(),
                  [&](size_t a, size_t b) {
                      return values[a] < values[b];
                  });

                  break;

              };
      case 'u': {

                  while (n != matr_idx[4][i]) {
                    i += 1;
                  }
                  if (i == matr_idx[4].size()) {
                    std::cerr << "Column not found.\n";
                    return;
                  };
                  const std::span<unsigned int> values(uint_v.data() + i * nrow, nrow);
                  std::sort(idx.begin(), idx.end(),
                  [&](size_t a, size_t b) {
                      return values[a] < values[b];
                  });

                  break;

                };
      case 'd': {

                  while (n != matr_idx[5][i]) {
                    i += 1;
                  }
                  if (i == matr_idx[5].size()) {
                    std::cerr << "Column not found.\n";
                    return;
                  };
                  const std::span<double> values(dbl_v.data() + i * nrow, nrow);
                  std::sort(idx.begin(), idx.end(),
                  [&](size_t a, size_t b) {
                      return values[a] < values[b];
                  });

                  break;

                };
      default: {
                 std::cerr << "Type unknowk in sort_by\n";
                 return;
               }
    };
  } else if constexpr (!asc) {
    switch(type_refv[n]) {
      case 's': {

                  while (n != matr_idx[0][i]) {
                    i += 1;
                  }

                  if (i == matr_idx[0].size()) {
                    std::cerr << "Column not found.\n";
                    return;
                  };

                  const std::span<std::string> values(str_v.data() + i * nrow, nrow);
                  std::sort(idx.begin(), idx.end(),
                  [&](size_t a, size_t b) {
                      return values[a] > values[b];
                  });

                  break;

                };
      case 'c': {

                  while (n != matr_idx[1][i]) {
                    i += 1;
                  }

                  if (i == matr_idx[1].size()) {
                    std::cerr << "Column not found.\n";
                    return;
                  };

                  const std::span<char> values(chr_v.data() + i * nrow, nrow);
                  std::sort(idx.begin(), idx.end(),
                  [&](size_t a, size_t b) {
                      return values[a] > values[b];
                  });

                  break;

                };
      case 'b': {

                  while (n != matr_idx[2][i]) {
                    i += 1;
                  }

                  if (i == matr_idx[2].size()) {
                    std::cerr << "Column not found.\n";
                    return;
                  };

                  std::vector<bool> values(bool_v.begin() + i * nrow, 
                                  bool_v.begin() + (i + 1) * nrow);
                  std::sort(idx.begin(), idx.end(),
                  [&](size_t a, size_t b) {
                      return values[a] > values[b];
                  });

                  break;

                };
      case 'i': {

                  while (n != matr_idx[3][i]) {
                    i += 1;
                  }

                  if (i == matr_idx[3].size()) {
                    std::cerr << "Column not found.\n";
                    return;
                  };

                  const std::span<int> values(int_v.data() + i * nrow, nrow);
                  std::sort(idx.begin(), idx.end(),
                  [&](size_t a, size_t b) {
                      return values[a] > values[b];
                  });

                  break;

              };
      case 'u': {

                  while (n != matr_idx[4][i]) {
                    i += 1;
                  }

                  if (i == matr_idx[4].size()) {
                    std::cerr << "Column not found.\n";
                    return;
                  };

                  const std::span<unsigned int> values(uint_v.data() + i * nrow, nrow);
                  std::sort(idx.begin(), idx.end(),
                  [&](size_t a, size_t b) {
                      return values[a] > values[b];
                  });

                  break;

                };
      case 'd': {

                  while (n != matr_idx[5][i]) {
                    i += 1;
                  }

                  if (i == matr_idx[5].size()) {
                    std::cerr << "Column not found.\n";
                    return;
                  };

                  const std::span<double> values(dbl_v.data() + i * nrow, nrow);
                  std::sort(idx.begin(), idx.end(),
                  [&](size_t a, size_t b) {
                      return values[a] > values[b];
                  });

                  break;

                };
      default: {
                 std::cerr << "Type unknowk in sort_by\n";
                 return;
               }
    };
  };
  unsigned int pos_vl;
  unsigned int pos_vl2;
  std::vector<std::string> tmp_str_vec = str_v;
  std::vector<std::string> str_v2(nrow);
  i2 = 0;
  for (auto& el : matr_idx[0]) {
    pos_vl = i2 * nrow;
    for (i = 0; i < nrow; i += 1) {
      pos_vl2 = idx[i];
      str_v2[i] = tmp_val_refv[el][pos_vl2];
      tmp_str_vec[pos_vl + i] = str_v[pos_vl + pos_vl2];
    };
    tmp_val_refv[el] = str_v2;
    i2 += 1;
  };
  str_v = tmp_str_vec;
  std::vector<char> tmp_chr_vec = chr_v;
  i2 = 0;
  for (auto& el : matr_idx[1]) {
    pos_vl = i2 * nrow;
    for (i = 0; i < nrow; i += 1) {
      pos_vl2 = idx[i];
      str_v2[i] = tmp_val_refv[el][pos_vl2];
      tmp_chr_vec[pos_vl + i] = chr_v[pos_vl + pos_vl2];
    };
    tmp_val_refv[el] = str_v2;
    i2 += 1;
  };
  chr_v = tmp_chr_vec;
  std::vector<bool> tmp_bool_vec = bool_v;
  i2 = 0;
  for (auto& el : matr_idx[2]) {
    pos_vl = i2 * nrow;
    for (i = 0; i < nrow; i += 1) {
      pos_vl2 = idx[i];
      str_v2[i] = tmp_val_refv[el][pos_vl2];
      tmp_bool_vec[pos_vl + i] = bool_v[pos_vl + pos_vl2];
    };
    tmp_val_refv[el] = str_v2;
    i2 += 1;
  };
  bool_v = tmp_bool_vec;
  std::vector<int> tmp_int_vec = int_v;
  i2 = 0;
  for (auto& el : matr_idx[3]) {
    pos_vl = i2 * nrow;
    for (i = 0; i < nrow; i += 1) {
      pos_vl2 = idx[i];
      str_v2[i] = tmp_val_refv[el][pos_vl2];
      tmp_int_vec[pos_vl + i] = int_v[pos_vl + pos_vl2];
    };
    tmp_val_refv[el] = str_v2;
    i2 += 1;
  };
  int_v = tmp_int_vec;
  std::vector<unsigned int> tmp_uint_vec = uint_v;
  i2 = 0;
  for (auto& el : matr_idx[4]) {
    pos_vl = i2 * nrow;
    for (i = 0; i < nrow; i += 1) {
      pos_vl2 = idx[i];
      str_v2[i] = tmp_val_refv[el][pos_vl2];
      tmp_uint_vec[pos_vl + i] = uint_v[pos_vl + pos_vl2];
    };
    tmp_val_refv[el] = str_v2;
    i2 += 1;
  };
  uint_v = tmp_uint_vec;
  std::vector<double> tmp_dbl_vec = dbl_v;
  i2 = 0;
  for (auto& el : matr_idx[5]) {
    pos_vl = i2 * nrow;
    for (i = 0; i < nrow; i += 1) {
      pos_vl2 = idx[i];
      str_v2[i] = tmp_val_refv[el][pos_vl2];
      tmp_dbl_vec[pos_vl + i] = dbl_v[pos_vl + pos_vl2];
    };
    tmp_val_refv[el] = str_v2;
    i2 += 1;
  };
  dbl_v = tmp_dbl_vec;
};


