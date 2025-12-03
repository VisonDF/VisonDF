#pragma once

template <typename T,
          unsigned int CORES = 4,
          bool SimdHash = true, 
          LeftJoinMethods Method = LeftJoinMethods::First>
void transform_left_join_mt(Dataframe &obj, 
                const unsigned int &key1, 
                const unsigned int &key2,
                const CharT, default_chr,
                const std::string default_str = "NA",
                const uint8_t default_bool = 0,
                const IntT default_int = 0,
                const UIntT default_uint = 0,
                const FloatT default_dbl = 0) 
{
  
    const unsigned int& ncol2 = obj.get_ncol();

    const std::vector<std::vector<unsigned int>>& matr_idx2 = obj.get_matr_idx();

    const auto& str_v2  = obj.get_str_vec();
    const auto& chr_v2  = obj.get_chr_vec();
    const auto& bool_v2 = obj.get_bool_vec();
    const auto& int_v2  = obj.get_int_vec();
    const auto& uint_v2 = obj.get_uint_vec();
    const auto& dbl_v2  = obj.get_dbl_vec();
    
    const unsigned int size_str  = matr_idx[0].size();
    const unsigned int size_chr  = matr_idx[1].size();
    const unsigned int size_bool = matr_idx[2].size();
    const unsigned int size_int  = matr_idx[3].size();
    const unsigned int size_uint = matr_idx[4].size();
    const unsigned int size_dbl  = matr_idx[5].size();
    
    const std::vector<char>& vec_type = obj.get_typecol();

    std::vector<std::vector<T>>* pre_col1 = nullptr;
    const std::vector<std::vector<T>>* pre_col2 = nullptr;
    size_t idx_type;
    if constexpr (std::is_same_v<T, std::string>)  {
        pre_col1 = &str_v;
        pre_col2 = &obj.get_str_v();
        idx_type = 0;
    } else if constexpr (std::is_same_v<T, CharT>) {
        pre_col1 = &chr_v;
        pre_col2 = &obj.get_chr_v();
        idx_type = 1;
    } else if constexpr (std::is_same_v<T, uint8_t>) {
        pre_col1 = &bool_v;
        pre_col2 = &obj.get_bool_v();
        idx_type = 2;
    } else if constexpr (std::is_same_v<T, IntT>) {
        pre_col1 = &int_v;
        pre_col2 = &obj.get_int_v();
        idx_type = 3;
    } else if constexpr (std::is_same_v<T, UIntT>) {
        pre_col1 = &uint_v;
        pre_col2 = &obj.get_uint_v();
        idx_type = 4;
    } else if constexpr (std::is_same_v<T, FloatT>) {
        pre_col1 = &dbl_v;
        pre_col2 = &obj.get_dbl_v();
        idx_type = 5;
    }

    size_t cur_idx = 0;
    while (key1 != matr_idx[idx_type][cur_idx]) ++cur_idx;
    const std::vector<T>& col1 = (*pre_col1)[cur_idx];
    cur_idx = 0;
    while (key2 != matr_idx2[idx_type][cur_idx]) ++cur_idx;
    const std::vector<T>& col2 = (*pre_col2)[cur_idx];
   
    str_v.resize(str_v.size()   + size_str);
    chr_v.resize(chr_v.size()   + size_chr);
    bool_v.resize(bool_v.size() + size_bool);
    int_v.resize(int_v.size()   + size_int);
    uint_v.resize(uint_v.size() + size_uint);
    dbl_v.resize(dbl_v.size()   + size_dbl);

    for (size_t i = 0; i < ncol2; ++i) {
        str_v [ncol + i].resize(nrow, default_str);
        chr_v [ncol + i].resize(nrow, default_chr);
        bool_v[ncol + i].resize(nrow, default_bool);
        int_v [ncol + i].resize(nrow, default_int);
        uint_v[ncol + i].resize(nrow, default_uint);
        dbl_v [ncol + i].resize(nrow, default_dbl);
    }

    std::vector<std::vector<unsigned int>> matr_idx2b = matr_idx2;
    for (auto& el : matr_idx2b) {
      for (auto& el2 : el) {
        el2 += ncol;
      };
    };

    matr_idx[0].insert(matr_idx[0].end(), 
                       matr_idx2b[0].begin(), 
                       matr_idx2b[0].end());
    matr_idx[1].insert(matr_idx[1].end(), 
                       matr_idx2b[1].begin(), 
                       matr_idx2b[1].end());
    matr_idx[2].insert(matr_idx[2].end(), 
                       matr_idx2b[2].begin(), 
                       matr_idx2b[2].end());
    matr_idx[3].insert(matr_idx[3].end(), 
                       matr_idx2b[3].begin(), 
                       matr_idx2b[3].end());
    matr_idx[4].insert(matr_idx[4].end(), 
                       matr_idx2b[4].begin(), 
                       matr_idx2b[4].end());
    matr_idx[5].insert(matr_idx[5].end(), 
                       matr_idx2b[5].begin(), 
                       matr_idx2b[5].end());

    using VALUE_TYPE = std::conditional_t<
        (Method == LeftJoinMethods::Aligned),
        MatchGroup,
        size_t>;

    using map_t = std::conditional_t<
        std::is_same_v<T, std::string>,
        std::conditional_t<
            SimdHash,
            ankerl::unordered_dense::map<std::string_view, VALUE_TYPE, simd_hash>,
            ankerl::unordered_dense::map<std::string_view, VALUE_TYPE>
        >,
        std::conditional_t<
            SimdHash,
            ankerl::unordered_dense::map<T, VALUE_TYPE, simd_hash>,
            ankerl::unordered_dense::map<T, VALUE_TYPE>
        >
    >;

    map_t lookup;
    lookup.reserve(col2.size());

    std::vector<size_t> match_idx(nrow, SIZE_MAX);
    const unsigned int nrow2;
    if constexpr (Method == LeftJoinMethods::First || 
                  Method == LeftJoinMethods::Last) {

        for (size_t i = 0; i < col2.size(); i += 1) {
            if constexpr (Method == LeftJoinMethods::First) {
                lookup.try_emplace(col2[i], i);
            } else if constexpr (Method == LeftJoinMethods::First) {
                lookup[col2[i]] = i;
            }
        };

        nrow2 = obj.get_nrow();

        for (size_t i = 0; i < nrow; ++i) {
            auto it = lookup.find(col1[i]);
            if (it != lookup.end())
                match_idx[i] = it->second;
        }

    } else if constexpr (Method == LeftJoinMethods::Aligned) {

        for (size_t i = 0; i < col2.size(); i += 1) {
          auto [it, inserted] = lookup.try_emplace(col2[i]);
          if (inserted) {
            it->second.idxs.reserve(2);
          }
          it->second.idxs.push_back(i);
        };

        nrow2 = obj.get_nrow();

        for (size_t i = 0; i < nrow; ++i) {
            auto it = lookup.find(col1[i]);
            if (it != lookup.end() && it->second.next < it->second.idxs.size()) {
                match_idx[i] = it->second.idxs[it->second.next];
                ++it->second.next;
            }
        }

    }

    auto copy_block = [&](auto& dst_v, auto& src_v, size_t size_offset, size_t type_idx) {
        for (size_t t = 0; t < matr_idx2[type_idx].size(); ++t) {
    
            auto* dst_val = dst_v[size_offset + t].data();
            const auto* src_val = src_v[t].data();
   
            #pragma omp parallel for if(CORES > 1) num_threads(CORES)
            for (size_t i = 0; i < nrow; ++i) {
                size_t j = match_idx[i];
                if (j != SIZE_MAX) {
                    dst_val[i] = src_val[j];
                }
            }
        }
    };

    copy_block(str_v,  str_v2,  size_str,  0);
    copy_block(chr_v,  chr_v2,  size_chr,  1);
    copy_block(bool_v, bool_v2, size_bool, 2);
    copy_block(int_v,  int_v2,  size_int,  3);
    copy_block(uint_v, uint_v2, size_uint, 4);
    copy_block(dbl_v,  dbl_v2,  size_dbl,  5); 

    type_refv.insert(type_refv.end(), vec_type.begin(), vec_type.end());
    ncol += ncol2;
    const std::vector<std::string>& colname2 = obj.get_colname();

    if (colname2.size() > 0) {
        name_v.insert(name_v.end(), colname2.begin(), colname2.end());
    } else {
        name_v.resize(ncol);
    };

};




