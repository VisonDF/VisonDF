#pragma once

template <typename T = void,
          unsigned int CORES = 4,
          bool SimdHash = true, 
          LeftJoinMethods Method = LeftJoinMethods::First>
void transform_left_join_mt(Dataframe &obj, 
                            const unsigned int key1, 
                            const unsigned int key2,
                            const CharT default_chr,
                            const std::string default_str = "NA",
                            const uint8_t default_bool = 0,
                            const IntT default_int = 0,
                            const UIntT default_uint = 0,
                            const FloatT default_dbl = 0) 
{
 
    if (in_view) {
        std::cerr << "Can't perform this operation while in" << 
                  "`view` mode, consider applying `.materialize()`\n";
        return;
    }

    const unsigned int& ncol2 = obj.get_ncol();

    const unsigned int local_nrow = nrow;

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
    
    str_v.resize(str_v.size()   + size_str);
    chr_v.resize(chr_v.size()   + size_chr);
    bool_v.resize(bool_v.size() + size_bool);
    int_v.resize(int_v.size()   + size_int);
    uint_v.resize(uint_v.size() + size_uint);
    dbl_v.resize(dbl_v.size()   + size_dbl);

    for (size_t i = 0; i < ncol2; ++i) {
        str_v [ncol + i].resize(local_nrow, default_str);
        chr_v [ncol + i].resize(local_nrow, default_chr);
        bool_v[ncol + i].resize(local_nrow, default_bool);
        int_v [ncol + i].resize(local_nrow, default_int);
        uint_v[ncol + i].resize(local_nrow, default_uint);
        dbl_v [ncol + i].resize(local_nrow, default_dbl);
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
            SimdHash,
            ankerl::unordered_dense::map<std::string_view, VALUE_TYPE, simd_hash>,
            ankerl::unordered_dense::map<std::string_view, VALUE_TYPE>
        >;

    map_t lookup;


    using key_variant_t = std::variant<
        std::monostate,
        std::vector<std::vector<std::string>>*,
        std::vector<std::vector<CharT>>*,
        std::vector<std::vector<uint8_t>>*,
        std::vector<std::vector<IntT>>*,
        std::vector<std::vector<UIntT>>*,
        std::vector<std::vector<FloatT>>*
    >; 
    key_variant_t var_key_table1;
    key_variant_t var_key_table2;
    size_t idx_type;

    if constexpr (!std::is_same_v<T, void>) {
        if constexpr (std::is_same_v<element_type_t<T>, std::string>)  {
            var_key_table1 = &str_v;
            var_key_table2 = &str_v2;
            idx_type = 0;
        } else if constexpr (std::is_same_v<element_type_t<T>, CharT>) {
            var_key_table1 = &chr_v;
            var_key_table2 = &chr_v2;
            idx_type = 1;
        } else if constexpr (std::is_same_v<element_type_t<T>, uint8_t>) {
            var_key_table1 = &bool_v;
            var_key_table2 = &bool_v2;
            idx_type = 2;
        } else if constexpr (std::is_same_v<element_type_t<T>, IntT>) {
            var_key_table1 = &int_v;
            var_key_table2 = &int_v2;
            idx_type = 3;
        } else if constexpr (std::is_same_v<element_type_t<T>, UIntT>) {
            var_key_table1 = &uint_v;
            var_key_table2 = &uint_v2;
            idx_type = 4;
        } else if constexpr (std::is_same_v<element_type_t<T>, FloatT>) {
            var_key_table1 = &dbl_v;
            var_key_table2 = &dbl_v2;
            idx_type = 5;
        }
    } else {
        switch (type_refv[key1]) {
            case 's': var_key_table1 = &str_v;  var_key_table2 = &str_v2;  idx_type = 0; break;
            case 'c': var_key_table1 = &chr_v;  var_key_table2 = &chr_v2;  idx_type = 1; break;
            case 'b': var_key_table1 = &bool_v; var_key_table2 = &bool_v2; idx_type = 2; break;
            case 'i': var_key_table1 = &int_v;  var_key_table2 = &int_v2;  idx_type = 3; break;
            case 'u': var_key_table1 = &uint_v; var_key_table2 = &uint_v2; idx_type = 4; break;
            case 'd': var_key_table1 = &dbl_v;  var_key_table2 = &dbl_v2;  idx_type = 5; break;
        }
    }

    size_t table_idx1;
    size_t table_idx2;

    table_idx1 = 0;
    table_idx2 = 0;
    while (key1 != matr_idx[idx_type][cur_idx]) ++cur_idx1;
    while (key2 != matr_idx2[idx_type][cur_idx]) ++cur_idx2;

    std::vector<size_t> match_idx(local_nrow, SIZE_MAX);

    std::visit([&lookup, 
                &match_idx, 
                &ob,
                &var_key_table2,
                val_size, 
                table_idx1,
                table_idx2](auto&& key_table) {

        using TP = std::decay_t<decltype(key_table1)>;

        if constexpr (!std::is_same_v<TP, std::monostate>) {

            using Elem = TP::value_type::value_type;

            const auto& col1 = (*key_table)[table_idx1];
            const auto& col2 = (*std::get<TP>(var_key_table2))[table_idx2];

            lookup.reserve(col2.size());

            if constexpr (Method == LeftJoinMethods::First || 
                          Method == LeftJoinMethods::Last) {

                if constexpr (std::is_same_v<Elem, std::string>) {
                    if constexpr (Method == LeftJoinMethod::First) {
                        for (size_t i = 0; i < col2.size(); i += 1) {
                            lookup.try_emplace(col2[i], i);
                        };
                    } else {
                        for (size_t i = 0; i < col2.size(); i += 1) {
                            lookup[col2[i]] = i;
                        };
                    }
                } else {
                   constexpr auto& size_table = get_types_size();
                   const size_t val_size = size_table[idx_type];
                   if constexpr (Method == LeftJoinMethod::First) {
                       for (size_t i = 0; i < col2.size(); i += 1) {
                           lookup.try_emplace(std::string_view{reinterpret_cast<const char*>(&col2[i]), 
                                                                   val_size}, i);
                       };
                   } else {
                       for (size_t i = 0; i < col2.size(); i += 1) {
                           lookup[std::string_view{reinterpret_cast<const char*>(&col2[i]), 
                                                                   val_size}] = i;
                       };
                   }
                }

                const unsigned int nrow2 = obj.get_nrow();

                for (size_t i = 0; i < local_nrow; ++i) {
                    auto it = lookup.find(col1[i]);
                    if (it != lookup.end())
                        match_idx[i] = it->second;
                }

            } else if constexpr (Method == LeftJoinMethods::Aligned) {

                if constexpr (std::is_same_v<Elem, std::string>) {
                    for (size_t i = 0; i < col2.size(); i += 1) {
                        auto [it, inserted] = lookup.try_emplace(col2[i], MatchGroup{});
                        if (inserted) {
                            it->second.idxs.reserve(3);
                        }
                        it->second.idxs.push_back(i);
                    };
                } else {
                    constexpr auto& size_table = get_types_size();
                    const size_t val_size = size_table[idx_type];
                    for (size_t i = 0; i < col2.size(); i += 1) {
                        auto [it, inserted] = lookup.try_emplace(std::string_view{reinterpret_cast<const char*>(&col2[i]), 
                                                                     val_size}, MatchGroup{});
                        if (inserted) {
                            it->second.idxs.reserve(3);
                        }
                        it->second.idxs.push_back(i);
                    };
                }

                const unsigned int nrow2 = obj.get_nrow();

                for (size_t i = 0; i < local_nrow; ++i) {
                    auto it = lookup.find(col1[i]);
                    if (it != lookup.end() && it->second.next < it->second.idxs.size()) {
                        match_idx[i] = it->second.idxs[it->second.next];
                        ++it->second.next;
                    }
                }

            }

        }

    }, var_key_table1);

    auto copy_block = [&matr_idx, &matr_idx2](auto& dst_v, auto& src_v, size_t size_offset, size_t idx_type) {
        for (size_t t = 0; t < matr_idx2[idx_type].size(); ++t) {
    
            auto* dst_val = dst_v[size_offset + t].data();
            const auto* src_val = src_v[t].data();
   
            #pragma omp parallel for if(CORES > 1) num_threads(CORES)
            for (size_t i = 0; i < local_nrow; ++i) {
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

    const std::vector<char>& vec_type = obj.get_typecol();

    type_refv.insert(type_refv.end(), vec_type.begin(), vec_type.end());
    ncol += ncol2;
    const std::vector<std::string>& colname2 = obj.get_colname();

    if (colname2.size() > 0) {
        name_v.insert(name_v.end(), colname2.begin(), colname2.end());
    } else {
        name_v.resize(ncol);
    };

};




