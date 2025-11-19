#pragma once

template <unsigned int CORES = 4, bool SimdHash = true>
void pivot_int_mt(Dataframe &obj, 
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

    for (auto& el : matr_idx2[3]) {
      if (n3 == el) {
        pos_val = nrow2 * i;
        break;
      };
      i += 1;
    };

    //std::unordered_map<std::pair<std::string_view, std::string_view>, int, PairHash> lookup; // standard map (slower)

    using fast_pair_str_map_t = std::conditional_t<
        SimdHash,
        ankerl::unordered_dense::map<std::pair<std::string_view, 
                                 std::string_view>, IntT, SimdPairHash>,
        ankerl::unordered_dense::map<std::pair<std::string_view, 
                                 std::string_view>, IntT, PairHash>
    >;

    fast_pair_str_map_t lookup;

    using fast_str_map_t = std::conditional_t<
        SimdHash,
        ankerl::unordered_dense::map<std::string_view, int, simd_hash>,
        ankerl::unordered_dense::map<std::string_view, int>
    >;

    //std::unordered_map<std::string_view, int> idx_col; // standard map (slower)
    fast_str_map_t idx_col;
    //std::unordered_map<std::string_view, int> idx_row;
    fast_str_map_t idx_row;
    
    idx_col.reserve(nrow2);
    idx_row.reserve(nrow2);
    lookup.reserve(nrow2);
    
    for (i = 0; i < nrow2; i += 1) {

      std::string_view col_key = col_vec[i];
      auto [col_it, col_inserted] = idx_col.try_emplace(col_key, idx_col.size());
      
      std::string_view row_key = row_vec[i];
      auto [row_it, row_inserted] = idx_row.try_emplace(row_key, idx_row.size());

      lookup[{col_key, row_key}] += cur_int_v[pos_val + i];
    };
    
    ncol = idx_row.size();
    nrow = idx_col.size();
    int_v.resize(ncol * nrow, 0);

    std::vector<std::string> cur_vec_str(nrow);
    tmp_val_refv.resize(ncol, cur_vec_str);

    #pragma omp parallel for schedule(static) num_threads(CORES)
    for (auto it = lookup.begin(); it < lookup.end(); ++it) {
        const auto& [key_pair, value] = *it;
        const auto& [col_key, row_key] = key_pair;
    
        const int col_idx = idx_col.at(col_key);
        const int row_idx = idx_row.at(row_key);
    
        int_v[col_idx * nrow + row_idx] = value;
    
        char buf[max_chars_needed<IntT>()];
        auto [ptr, ec] = std::to_chars(buf, buf + sizeof(buf), value);
        tmp_val_refv[col_idx][row_idx].assign(buf, ptr - buf);
    }

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


