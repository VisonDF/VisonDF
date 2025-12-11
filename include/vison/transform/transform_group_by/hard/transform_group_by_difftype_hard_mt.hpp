#pragma once

template <unsigned int CORES = 4,
          bool Occurence = false,
          bool SimdHash = true>
void transform_group_by_difftype_hard_mt(const std::vector<unsigned int>& x,
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
                                     PairGroupBy<value_t>, 
                                     simd_hash>,
        ankerl::unordered_dense::map<std::string, 
                                     PairGroupBy<value_t>>
    >;

    const unsigned int local_nrow = nrow;
    std::vector<unsigned int> x2(x.size());

    idx_str.reserve(x.size()  / 2);
    idx_chr.reserve(x.size()  / 2);
    idx_bool.reserve(x.size() / 2);
    idx_int.reserve(x.size()  / 2);
    idx_uint.reserve(x.size() / 2);
    idx_dbl.reserve(x.size()  / 2);

    for (int v : x) {
        auto it = std::find(matr_idx[0].begin(), matr_idx[0].end(), v);
        if (it != matr_idx[0].end())
            idx_str.push_back(std::distance(matr_idx[0].begin(), it));
    }
    for (int v : x) {
        auto it = std::find(matr_idx[1].begin(), matr_idx[1].end(), v);
        if (it != matr_idx[1].end())
            idx_chr.push_back(std::distance(matr_idx[1].begin(), it));
    }
    for (int v : x) {
        auto it = std::find(matr_idx[2].begin(), matr_idx[2].end(), v);
        if (it != matr_idx[2].end())
            idx_bool.push_back(std::distance(matr_idx[2].begin(), it));
    }
    for (int v : x) {
        auto it = std::find(matr_idx[3].begin(), matr_idx[3].end(), v);
        if (it != matr_idx[3].end())
            idx_int.push_back(std::distance(matr_idx[3].begin(), it));
    }
    for (int v : x) {
        auto it = std::find(matr_idx[4].begin(), matr_idx[4].end(), v);
        if (it != matr_idx[4].end())
            idx_uint.push_back(std::distance(matr_idx[4].begin(), it));
    }
    for (int v : x) {
        auto it = std::find(matr_idx[5].begin(), matr_idx[5].end(), v);
        if (it != matr_idx[5].end())
            idx_dbl.push_back(std::distance(matr_idx[5].begin(), it));
    }

    std::sort(idx_str.begin(),  idx_str.end()); 
    std::sort(idx_chr.begin(),  idx_chr.end());
    std::sort(idx_bool.begin(), idx_bool.end());
    std::sort(idx_int.begin(),  idx_int.end());
    std::sort(idx_uint.begin(), idx_uint.end());
    std::sort(idx_dbl.begin(),  idx_dbl.end());

    map_t lookup;
    lookup.reserve(local_nrow);

    std::string key;
    key.reserve(2048);   
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
    key_variant_t key_table2 = nullptr;

    size_t n_col_real;
    if constexpr (!Occurence) {
        switch (type_refv[x[0]]) {
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

    auto build_key = [&] (std::string& key, unsigned int i) {
        for (auto idxv : idx_str) {
            const auto& v = str_v[idxv][i];
            key.append(
                       v.data(),
                       v.size()
            );
            key.push_back('\x1F');              
        }
        for (auto idxv : idx_chr) {
            const auto& v = chr_v[idxv][i];
            key.append(
                       v,
                       sizeof(v)
            );
            key.push_back('\x1F');              
        }
        for (auto idxv : idx_bool) {
            const auto& v = bool_v[idxv][i];
            key.append(
                       reinterpret_cast<const char*>(&v),
                       sizeof(v)
            );
            key.push_back('\x1F');              
        }
        for (auto idxv : idx_int) {
            const auto& v = int_v[idxv][i];
            key.append(
                       reinterpret_cast<const char*>(&v),
                       sizeof(v)
            );
            key.push_back('\x1F');              
        }
        for (auto idxv : idx_uint) {
            const auto& v = uint_v[idxv][i];
            key.append(
                       reinterpret_cast<const char*>(&v),
                       sizeof(v)
            );
            key.push_back('\x1F');              
        }
        for (auto idxv : idx_dbl) {
            const auto& v = dbl_v[idxv][i];
            key.append(
                       reinterpret_cast<const char*>(&v),
                       sizeof(v)
            );
            key.push_back('\x1F');              
        }
    }

    if constexpr (CORES == 1) {
        for (unsigned int i = 0; i < local_nrow; ++i) { 
            key.clear();
            key_build(key, i);
            auto [it, inserted] = lookup.try_emplace(key, 0);
            auto& cur_struct = it->second;
            if constexpr (Occurence) {
                ++cur_struct.value;
            } else if constexpr (!Occurence) {
                cur_struct.value += (*key_table2)[n_col_real][i];
            }
            cur_struct.idx_vec.push_back(i);
        }
    } else if constexpr (CORES > 1) {    
        const unsigned int chunks = local_nrow / CORES + 1;
        std::vector<map_t> vec_map(CORES);
        #pragma omp parallel num_threads(CORES)
        {
            std::string key;
            key.reserve(2048);
            const unsigned int tid   = omp_get_thread_num();
            const unsigned int start = tid * chunks;
            const unsigned int end   = std::min(local_nrow, start + chunks);
            map_t& cur_map           = vec_map[tid];
            cur_map.reserve(local_nrow / CORES);
            for (size_t i = start; i < end; ++i) {
                key.clear();
                key_build(key, i);
                auto [it, inserted] = cur_map.try_emplace(key, 0);
                auto& cur_struct = it->second;
                if constexpr (Occurence) {
                    ++(cur_struct.value);
                } else if constexpr (!Occurence) {
                    cur_struct.value += (*key_table2)[n_col_real][i];
                }
                cur_struct.idx_vec.push_back(i);
            }
        }
        for (auto& cur_map : vec_map) {
            for (auto& [k, v] : cur_map) {
                auto [it, inserted] = lookup.try_emplace(k, 0);
                auto& cur_struct = it->second;
                if constexpr (Occurence) {
                    cur_struct.value += v.value;
                } else if constexpr (!Occurence) {
                    cur_struct.value += v.value;
                }
                if constexpr (std::is_trivially_copyable<T>) {
                     const unsigned int n_old_size = cur_struct.idx_vec.size();
                     cur_struct.idx_vec.resize(cur_struct.idx_vec.size() + v.idx_vec.size());
                     memcpy(cur_struct.idx_vec.data() + n_old_size,
                            v.idx_vec.data(),
                            v.idx_vec.size() * sizeof(T)
                            );
                } else {
                    cur_struct.idx_vec.reserve(cur_struct.idx_vec.size() + v.idx_vec.size());
                    cur_struct.idx_vec.insert(cur_struct.idx_vec.end(), 
                                              v.idx_vec.begin(), 
                                              v.idx_vec.end()
                                              );
                }
            }
        }
        #pragma omp parallel for num_threads(CORES)
        for (size_t i = 0; i < local_nrow; ++i) {
            std::string key;
            key.reserve(2048);
            key_build(key, i);
            key_vec[i] = &lookup.find(key)->first;
        }
    }

    std::vector<value_t> value_col(local_nrow);

    if constexpr (CORES > 1) {
        using group_vec_t = std::vector<unsigned int>;
        
        std::vector<size_t> pos_boundaries;
        pos_boundaries.reserve(lookup.size() + 1);
        pos_boundaries.push_back(0);
        
        for (auto it = lookup.begin(); it != lookup.end(); ++it) {
            pos_boundaries.push_back(
                pos_boundaries.back() + it->second.idx_vec.size()
            );
        }
        
        auto it0 = lookup.begin();
        
        #pragma omp parallel for num_threads(CORES) schedule(static)
        for (size_t g = 0; g < lookup.size(); ++g) {
            size_t start = pos_boundaries[g];
            size_t len   = pos_boundaries[g + 1] - pos_boundaries[g];
            const group_vec_t& vec = (it0 + g)->second.idx_vec;
            const auto& cur_val    = (it0 + g)->second.value;
            for (size_t t = 0; t < vec.size(); ++t)
                value_col[start + t] = cur_val;
            memcpy(row_view_idx.data() + start,
                   vec.data(),
                   len * sizeof(unsigned int));
        }
    } else {
        auto it = lookup.begin();
        size_t i2 = 0;
        for (size_t i = 0; i < lookup.size(); ++i) {
            const auto& pos_vec = (it + i)->second.idx_vec;
            const auto& cur_val = (it + i)->second.value;
            for (size_t t = 0; t < pos_vec.size(); ++t)
                value_col[i2 + t] = cur_val;
            memcpy(row_view_idx.data() + i2, 
                   pos_vec.data(), 
                   sizeof(unsigned int) * pos_vec.size());
            i2 += pos_vec.size();
        }
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

    col_alrd_materialized.push_back(ncol);

    if (!name_v.empty())
        name_v.push_back(colname);

    type_refv.push_back('u');
    ++ncol;
}




