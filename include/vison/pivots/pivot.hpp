#pragma once

template <typename T,
          bool SimdHash = true>
void pivot(Dataframe &obj, 
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

    const std::vector<std::vector<T>>* cur_col = nullptr;
    if constexpr (std::is_same_v<T, IntT>) {
        cur_col = &obj.get_int_vec();
    } else if constexpr (std::is_same_v<T, UIntT>) {
        cur_col = &obj.get_uint_vec();
    } else if constexpr (std::is_same_v<T, FloatT>) {
        cur_col = &obj.get_dbl_vec();
    } else {
        std::cerr << "Unsupported type for pivots\n";
        return;
    }

    for (auto& el : matr_idx2[5]) {
        if (n3 == el) {
            pos_val = i;
            break;
        };
        i += 1;
    };
    const auto& cur_col_v = (*cur_col)[pos_val];

    using fast_pair_str_map_t = std::conditional_t<
        SimdHash,
        ankerl::unordered_dense::map<std::pair<std::string_view, 
                                 std::string_view>, T, SimdPairHash>,
        ankerl::unordered_dense::map<std::pair<std::string_view, 
                                 std::string_view>, T, PairHash>
    >;

    fast_pair_str_map_t lookup;

    using fast_str_map_t = std::conditional_t<
        SimdHash,
        ankerl::unordered_dense::map<std::string_view, int, simd_hash>,
        ankerl::unordered_dense::map<std::string_view, int>
    >;

    fast_str_map_t idx_col;
    fast_str_map_t idx_row;
    
    idx_col.reserve(nrow2);
    idx_row.reserve(nrow2);
    lookup.reserve(nrow2);
    
    for (i = 0; i < nrow2; i += 1) {

        //try to add if not already exists
        std::string_view col_key = col_vec[i];
        auto [col_it, col_inserted] = idx_col.try_emplace(col_key, idx_col.size());
        
        std::string_view row_key = row_vec[i];
        auto [row_it, row_inserted] = idx_row.try_emplace(row_key, idx_row.size());

        lookup[{col_key, row_key}] += cur_col_v[i];
    };
    
    ncol = idx_row.size();
    nrow = idx_col.size();
    const unsigned int local_nrow = nrow;

    std::vector<std::vector<T>>* cols = nullptr;
    if constexpr (std::is_same_v<T, IntT>) {
        idx_type = 3;
        cols = &int_v_view;
    } else if constexpr (std::is_same_v<T, UIntT>) {
        idx_type = 4;
        cols = &uint_v_view;
    } else if constexpr (std::is_same_v<T, FloatT>) {
        idx_type = 5;
        cols = &dbl_v_view;
    }

    (*cols).resize(ncol, 0);
    for (auto& el : *cols)
        el.resize(local_nrow);

    std::vector<std::string> cur_vec_str(local_nrow);
    tmp_val_refv.resize(ncol, cur_vec_str);

    constexpr size_t buf_size = max_chars_needed<T>();
    char buf[buf_size];
    
    for (const auto& [key_pair, value] : lookup) {
        const auto& [col_key, row_key] = key_pair;
    
        const int col_idx = idx_col[col_key];
        const int row_idx = idx_row[row_key];
    
        (*cols)[col_idx][row_idx] = value;
    
        auto [ptr, ec] = fast_to_chars(buf, buf + sizeof(buf), value);
        tmp_val_refv[col_idx][row_idx].assign(buf, ptr - buf);
    }
    
    name_v.resize(idx_col.size());
    i = 0;
    matr_idx[idx_type].resize(ncol);
    for (auto& [key_v, value] : idx_col) {
        type_refv.push_back('d');
        name_v[value] = key_v;
        matr_idx[idx_type][i] = i;
        i += 1;
    };
    
    name_v_row.resize(idx_row.size()); 
    for (auto& [key_v, value] : idx_row) {
        name_v_row[value] = key_v;
    };

};



