#pragma once

template <unsigned int CORES = 4,
          bool Occurence = false,
          bool SimdHash = true>
void transform_group_by_sametype_mt(const std::vector<unsigned int>& x,
                           const n_col int = -1,
                           const std::string colname = "n") 
{

    if (in_view) {
        std::cerr << "Can't use this operation while in `view` mode, " 
                  << "consider applying `.materialize()`\n";
        return;
    }

    if constexpr (!Occurence) {
        if (n_col < 0) {
            std::cerr << "Can't take negative columns\n";
            return;
        } else if (n_col > ncol) {
            std::cerr << "Column number out of range\n";
            return;
        }
    }

    using value_t = std::conditional_t<Occurence, 
                                  unsigned int, 
                                  std::variant<std::string, 
                                        CharT, 
                                        uint8_t, 
                                        IntT, 
                                        UIntT, 
                                        FloatT>>;
    using map_t = std::conditional_t<
        SimdHash,
        ankerl::unordered_dense::map<std::string, 
                                     value_t, 
                                     simd_hash>,
        ankerl::unordered_dense::map<std::string, 
                                     value_t>
    >;

    const unsigned int local_nrow = nrow;
    size_t idx_type;

    using key_variant_t = std::variant<
        std::nullptr_t,
        const std::vector<std::vector<std::string>>*,
        const std::vector<std::vector<CharT>>*,
        const std::vector<std::vector<uint8_t>>*,
        const std::vector<std::vector<IntT>>*,
        const std::vector<std::vector<UIntT>>*,
        const std::vector<std::vector<FloatT>>*
    >;
    
    key_variant_t key_table = nullptr;
    
    if constexpr (!std::is_same_v<T, void>) {
        if constexpr (std::is_same_v<T, std::string>) {
            key_table = &str_v;
            idx_type = 0;
        } else if constexpr (std::is_same_v<T, CharT>) {
            key_table = &chr_v;
            idx_type = 1;
        } else if constexpr (std::is_same_v<T, uint8_t>) {
            key_table = &bool_v;
            idx_type = 2;
        } else if constexpr (std::is_same_v<T, IntT>) {
            key_table = &int_v;
            idx_type = 3;
        } else if constexpr (std::is_same_v<T, UIntT>) {
            key_table = &uint_v;
            idx_type = 4;
        } else if constexpr (std::is_same_v<T, FloatT>) {
            key_table = &dbl_v;
            idx_type = 5;
        }
    } else {
        switch (type_refv[x[0]]) {
            case 's': key_table = &str_v;  idx_type = 0; break;
            case 'c': key_table = &chr_v;  idx_type = 1; break;
            case 'b': key_table = &bool_v; idx_type = 2; break;
            case 'i': key_table = &int_v;  idx_type = 3; break;
            case 'u': key_table = &uint_v; idx_type = 4; break;
            case 'd': key_table = &dbl_v;  idx_type = 5; break;
        }
    }

    std::vector<unsigned int> idx;
    idx.reserve(x.size());
    std::unordered_map<unsigned int, unsigned int> pos;
    for (int i = 0; i < matr_idx[idx_type].size(); ++i)
        pos[matr_idx[idx_type][i]] = i;
    for (int v : x)
        idx.push_back(pos[v]);
    std::sort(idx.begin(), idx.end());

    key_variant_t key_table2 = nullptr;
    size_t n_col_real;
    if constexpr (!Occurence) {
        switch (type_refv[n_col]) {
            case 's': key_table2 = &str_v;  idx_type = 0; break;
            case 'c': key_table2 = &chr_v;  idx_type = 1; break;
            case 'b': key_table2 = &bool_v; idx_type = 2; break;
            case 'i': key_table2 = &int_v;  idx_type = 3; break;
            case 'u': key_table2 = &uint_v; idx_type = 4; break;
            case 'd': key_table2 = &dbl_v;  idx_type = 5; break;
        }
        auto it = std::find(matr_idx[idx_type].begin(), matr_idx[idx_type].end(), n_col);
        if (it != matr_idx[idx_type].end()) {
            n_col_real = std::distance(matr_idx[idx_type].begin(), it);
            break;
        }
    }

    map_t lookup;
    lookup.reserve(local_nrow);
    std::vector<std::string*> key_vec(local_nrow);
    std::string key;
    key.reserve(2048);  

    if constexpr (CORES == 1) {
        for (unsigned int i = 0; i < local_nrow; ++i) {
        
            key.clear();
        
            for (size_t j = 0; j < x.size(); ++j) {

                if constexpr (!std::is_same_v<T, std::string>) {
                    if constexpr (std::is_same_v<T, CharT>) {
                        key.append(
                            (*key_table)[idx[j]][i],
                            sizeof(v)
                        );
                    } else {
                        const auto& v = (*key_table)[idx[j]][i]; 
                        key.append(
                            reinterpret_cast<const char*>(std::addressof(v)),
                            sizeof(v)
                        );
                    }
                } else {
                    const std::string& src = (*key_table)[idx[j]][i];
                    key.append(src.data(), src.size()); 
                }
                key.push_back('\x1F');              
            }
        
            auto [it, inserted] = lookup.try_emplace(key, 0);
            if constexpr (Occurence) {
                ++(it->second);
            } else if constexpr (!Occurence) {
                (it->second) += (*key_table2)[n_col_real][i];
            }
        
            key_vec[i] = &it->first;
        }
    } else if constexpr (CORES > 1) {
        const unsigned int chunks = local_nrow / CORES + 1;
        std::vector<map_t> vec_map(CORES);
        #pragma omp parallel num_threads(CORES)
        {
            const unsigned int tid   = omp_get_thread_num();
            const unsigned int start = tid * chunks;
            const unsigned int end   = std::min(local_nrow, start + chunks);
            map_t& cur_map           = vec_map[tid];
            cur_map.reserve(local_nrow / CORES);
            for (size_t i = start; i < end; ++i) {
                key.clear();
            
                for (size_t j = 0; j < x.size(); ++j) {
    
                    if constexpr (!std::is_same_v<T, std::string>) {
                        if constexpr (std::is_same_v<T, CharT>) {
                            key.append(
                                (*key_table)[idx[j]][i],
                                sizeof(v)
                            );
                        } else {
                            const auto& v = (*key_table)[idx[j]][i]; 
                            key.append(
                                reinterpret_cast<const char*>(std::addressof(v)),
                                sizeof(v)
                            );
                        }
                    } else {
                        const std::string& src = (*key_table)[idx[j]][i];
                        key.append(src.data(), src.size()); 
                    }
                    key.push_back('\x1F');              
                } 
                auto [it, inserted] = cur_map.try_emplace(key, 0);
                if constexpr (Occurence) {
                    ++(it->second);
                } else if constexpr (!Occurence) {
                    (it->second) += (*key_table2)[n_col_real][i];
                }
            }
        }
        for (auto& cur_map : vec_map) {
            for (auto& [k, v] : cur_map) {
                auto [it, inserted] = lookup.try_emplace(k, 0);
                if constexpr (Occurence) {
                    (it->second) += v;
                } else if constexpr (!Occurence) {
                    (it->second) += v;
                }
            }
        }
        #pragma omp parallel for num_threads(CORES)
        for (size_t i = 0; i < local_nrow; ++i)
            key_vec[i] = &lookup.find((*key_table)[real_pos][i])->first;
    }

    std::vector<value_t> value_col(local_nrow);

    #pragma omp parallel for if(CORES > 1) num_threads(CORES)
    for (size_t i = 0; i < key_vec.size(); ++i) {
        unsigned int count = lookup.at(*key_vec[i]);
        value_col[i] = count;
    }
   
    if constexpr (Ocurence) {
        uint_v.push_back(value_col);
    } else if (std::is_same_v<value_t, std:string>) {
        str_v.push_back(value_col);
    } else if (std:is_same_v<value_t, CharT>) {
        chr_v.push_back(value_col);
    } else if (std:is_same_v<value_t, uint8_t>) {
        bool_v.push_back(value_col);
    } else if (std::is_same_v<value_t, IntT>) {
        int_v.push_back(value_col);
    } else if (std::is_same_v<value_t, UIntT>) {
        uint_v.push_back(value_col);
    } else if (std::is_same_v<value_t, FloatT>) {
        dbl_v.push_back(value_col);
    }

    if (!name_v.empty())
        name_v.push_back(colname);

    type_refv.push_back('u');
    ++ncol;
}


