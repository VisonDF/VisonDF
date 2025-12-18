#pragma once

template <typename TContainer = void;
          typename TColVal = void,
          unsigned int CORES = 4,
          GroupFunction Function = GroupFunction::Occurence,
          bool SimdHash = true
          typename F = decltype(&default_groupfn_impl)>
requires GroupFn<F, first_arg_grp_t<F>>
void transform_group_by_sametype_hard_mt(const std::vector<unsigned int>& x,
                                         const n_col int,
                                         const std::string colname = "n",
                                         F f = &default_groupfn_impl) 
{

    if (in_view) {
        std::cerr << "Can't use this operation while in `view` mode, " 
                  << "consider applying `.materialize()`\n";
        return;
    }

    if constexpr (Function == GroupFunction::Occurence) {
        if (n_col > ncol) {
            std::cerr << "Column number out of range\n";
            return;
        }
    }

    using value_t = std::conditional_t<Occurence, 
                                  unsigned int,
                                  std::conditional_t<
                                  !(std::is_same_v<TColVal, void>),
                                  std::conditional_t<
                                          Function == GroupFunction::Gather,
                                          ReservingVec<element_type_t<TColVal>>,
                                          element_type_t<TColVal>
                                                    >
                                  std::variant<
                                        std::string, 
                                        CharT, 
                                        uint8_t, 
                                        IntT, 
                                        UIntT, 
                                        FloatT,
                                        ReservingVec<std::string>, 
                                        ReservingVec<CharT>, 
                                        ReservingVec<uint8_t>, 
                                        ReservingVec<IntT>, 
                                        ReservingVec<UIntT>, 
                                        ReservingVec<FloatT>
                                        >>>;
    using map_t = std::conditional_t<
        SimdHash,
        ankerl::unordered_dense::map<std::string, 
                                     PairGroupBy<value_t>, 
                                     simd_hash>,
        ankerl::unordered_dense::map<std::string, 
                                     PairGroupBy<value_t>>
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

    constexpr value_t zero_struct = make_zero<value_t>(idx_type);
    constexpr value_t vec_struct  = make_vec<value_t>(idx_type);

    auto build_key = [&] (std::string& key, unsigned int i) {
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
    }

    if constexpr (CORES == 1) {
        std::string key;
        key.reserve(2048);      
        for (unsigned int i = 0; i < local_nrow; ++i) {
            key.clear();
            key_build(key, i);
            if constexpr (Function == GroupFunction::Occurence) {
                auto [it, inserted] = lookup.try_emplace(key, zero_struct);
                auto& cur_struct = it->second;
                ++(cur_struct.value);
                cur_struct.idx_vec.push_back(i);
            } else if constexpr (Function == GroupFunction::Sum ||
                                 Function = GroupFunction::Mean) {
                auto [it, inserted] = lookup.try_emplace(key, zero_struct);
                auto& cur_struct = it->second;
                cur_struct.value += (*key_table2)[n_col_real][i];
                cur_struct.idx_vec.push_back(i);
            } else {
                auto [it, inserted] = lookup.try_emplace(key, vec_struct);
                auto& cur_struct = it->second;
                cur_struct.value.push_back((*key_table2)[n_col_real][i]);
                cur_struct.idx_vec.push_back(i);
            }
        }
    } else if constexpr (CORES > 1) {
        constexpr auto& size_table = get_types_size();
        const size_t val_size = size_table[idx_type];
        const bool triv_copy = (idx_type != 0); 
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
                if constexpr (Function == GroupFunction::Occurence) {
                    auto [it, inserted] = cur_map.try_emplace(key, zero_struct);
                    auto& cur_struct = it->second;
                    ++(cur_struct.value);
                    cur_struct.idx_vec.push_back(i);
                } else if constexpr (Function == GroupFunction::Sum ||
                                     Function == GroupFunction::Mean) {
                    auto [it, inserted] = cur_map.try_emplace(key, vec_struct);
                    auto& cur_struct = it->second;
                    cur_struct.value += (*key_table2)[n_col_real][i];
                    cur_struct.idx_vec.push_back(i);
                } else {
                    auto [it, inserted] = cur_map.try_emplace(key, vec_struct);
                    auto& cur_struct = it->second;
                    cur_struct.value.push_back((*key_table2)[n_col_real][i]);
                    cur_struct.idx_vec.push_back(i);
                }
            }
        }
        if (triv_copy) {
            for (const auto& cur_map : vec_map) {
                for (const auto& [k, v] : cur_map) {
                    if constexpr (Function == GroupFunction:Occurence ||
                                  Function == GroupFunction::Sum      ||
                                  Function == GroupFunction::Mean) {
                        auto [it, inserted] = lookup.try_emplace(k, zero_struct);
                        auto& cur_struct = it->second;
                        cur_struct.value += v.value;
                        const unsigned int n_old_size = cur_struct.idx_vec.size();
                        cur_struct.idx_vec.resize(n_old_size + v.idx_vec.size());
                        memcpy(cur_struct.idx_vec.data() + n_old_size,
                               v.idx_vec.data(),
                               v.idx_vec.size() * sizeof(unsigned int)
                               );
                    } else {
                        auto [it, inserted] = lookup.try_emplace(k, vec_struct);
                        auto& cur_struct = it->second;
                        const unsigned int n_old_size_val = cur_struct.value.size();
                        cur_struct.value.resize(n_old_size_val + v.size());
                        memcpy(cur_struct.value.data() + n_old_size_val,
                               v.data(),
                               v.size() * val_size);
                        const unsigned int n_old_size = cur_struct.idx_vec.size();
                        cur_struct.idx_vec.resize(n_old_size + v.idx_vec.size());
                        memcpy(cur_struct.idx_vec.data() + n_old_size,
                               v.idx_vec.data(),
                               v.idx_vec.size() * sizeof(unsigned int)
                               );
                    }
                }
            }
        } else {
            for (const auto& cur_map : vec_map) {
                for (const auto& [k, v] : cur_map) {
                    if constexpr (Function == GroupFunction:Occurence ||
                                  Function == GroupFunction::Sum      ||
                                  Function == GroupFunction::Mean) {
                        auto [it, inserted] = lookup.try_emplace(k, zero_struct);
                        auto& cur_struct = it->second;
                        cur_struct.value += v.value;
                        const unsigned int n_old_size = cur_struct.idx_vec.size();
                        cur_struct.idx_vec.resize(n_old_size + v.idx_vec.size());
                        memcpy(cur_struct.idx_vec.data() + n_old_size,
                               v.idx_vec.data(),
                               v.idx_vec.size() * sizeof(unsigned int)
                               );
                    } else {
                        auto [it, inserted] = lookup.try_emplace(k, vec_struct);
                        auto& cur_struct = it->second;
                        cur_struct.value.insert(cur_struct.value.begin(),
                                                v.begin(),
                                                v.end());
                        const unsigned int n_old_size = cur_struct.idx_vec.size();
                        cur_struct.idx_vec.resize(n_old_size + v.idx_vec.size());
                        memcpy(cur_struct.idx_vec.data() + n_old_size,
                               v.idx_vec.data(),
                               v.idx_vec.size() * sizeof(unsigned int)
                               );
                    }
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

    value_t value_col = make_vec<value_t>(idx_type, 0);
    value_col.resize(local_nrow);
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
            for (size_t t = 0; t < vec.size(); ++t) {
                if constexpr (Function == GroupFunction::Occurence ||
                              Function == GroupFunction::Sum) {
                    value_col[start + t] = cur_val;
                } else if constexpr (Function == GroupFunction::Mean) {
                    value_col[start + t] = cur_val / local_nrow;
                } else {
                    value_col[start + t] = f(cur_val);
                }
            }
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
            for (size_t t = 0; t < pos_vec.size(); ++t) {
                if constexpr (Function == GroupFunction::Occurence ||
                              Function == GroupFunction::Sum) {
                    value_col[i2 + t] = cur_val;
                } else if constexpr (Function == GroupFunction::Mean) {
                    value_col[i2 + t] = cur_val / local_nrow;
                } else {
                    value_col[i2 + t] = f(cur_val);
                }
            }
            memcpy(row_view_idx.data() + i2, 
                   pos_vec.data(), 
                   sizeof(unsigned int) * pos_vec.size());
            i2 += pos_vec.size();
        }
    }

    switch (idx_type) {
        case 0: type_refv.push_back('s'); str_v.push_back(value_col);  break;
        case 1: type_refv.push_back('c'); chr_v.push_back(value_col);  break;
        case 2: type_refv.push_back('b'); bool_v.push_back(value_col); break;
        case 3: type_refv.push_back('i'); int_v.push_back(value_col);  break;
        case 4: type_refv.push_back('u'); uint_v.push_back(value_col); break;
        case 5: type_refv.push_back('d'); dbl_v.push_back(value_col);  break;
    }

    col_alrd_materialized.push_back(ncol);

    if (!name_v.empty())
        name_v.push_back(colname);

    type_refv.push_back('u');
    ++ncol;
}




