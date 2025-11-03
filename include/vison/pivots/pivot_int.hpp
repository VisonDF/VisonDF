#pragma once

void pivot_int(Dataframe &obj, 
                unsigned int &n1, 
                unsigned int& n2, 
                unsigned int& n3) 
{
  const std::vector<std::vector<std::string>>& tmp = obj.get_tmp_val_refv();
  const std::vector<std::string>& col_vec = tmp[n1];
  const std::vector<std::string>& row_vec = tmp[n2];
  const unsigned int& nrow2 = obj.get_nrow();
  const std::vector<std::vector<unsigned int>>& matr_idx2 = obj.get_matr_idx();
  unsigned int i = 0;
  unsigned int pos_val;
  const std::vector<int>& cur_int_v = obj.get_int_vec();

  std::vector<IntT> tmp_int_v = {};

  //std::unordered_map<std::pair<std::string_view, std::string_view>, int, PairHash> lookup; // standard map (slower)
  ankerl::unordered_dense::map<std::pair<std::string_view, std::string_view>, IntT, PairHash> lookup;

  std::string key;
  for (auto& el : matr_idx2[3]) {
    if (n3 == el) {
      pos_val = nrow2 * i;
      tmp_int_v.insert(tmp_int_v.end(), 
                      cur_int_v.begin() + pos_val, 
                      cur_int_v.begin() + pos_val + nrow2);
    };
    i += 1;
  };
  //std::unordered_map<std::string, int> idx_col; // standard map (slower)
  ankerl::unordered_dense::map<std::string, int> idx_col;
  //std::unordered_map<std::string, int> idx_row;
  ankerl::unordered_dense::map<std::string, int> idx_row;

  for (i = 0; i < nrow2; i += 1) {
    key = col_vec[i];
    if (!idx_col.contains(key)) {
      idx_col[key] = idx_col.size();
    };
    key = row_vec[i];
    if (!idx_row.contains(key)) {
      idx_row[key] = idx_row.size();
    };
    lookup[{col_vec[i], row_vec[i]}] += tmp_int_v[i];
  };
  ncol = idx_row.size();
  nrow = idx_col.size();
  int_v.resize(ncol * nrow);

  std::vector<std::string> cur_vec_str(idx_row.size());
  tmp_val_refv.resize(ncol, cur_vec_str);
 
  IntT cur_int;      
  for (const auto& [key_v, value] : idx_col) {
    for (const auto& [key_v2, value2] : idx_row) {
      auto key_pair = std::pair<std::string_view, std::string_view> {key_v, key_v2};
      if (lookup.contains(key_pair)) {
        cur_int = lookup[key_pair];
        int_v[value * nrow + value2] = cur_int;
        tmp_val_refv[value][value2] = std::to_string(cur_int);
      };
    };
  };
  name_v.resize(idx_col.size());
  i = 0;
  matr_idx[3].resize(ncol);
  for (auto& [key_v, value] : idx_col) {
    type_refv.push_back('i');
    name_v[value] = key_v;
    matr_idx[3][i] = i;
    i += 1;
  };
  name_v_row.resize(idx_row.size());
  for (auto& [key_v, value] : idx_row) {
    name_v_row[value] = key_v;
  };
};


