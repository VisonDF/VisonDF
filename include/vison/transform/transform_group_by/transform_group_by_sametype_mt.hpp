#pragma once

template <typename TContainer = void,
          typename TColVal = void,
          unsigned int CORES = 4,
          GroupFunction Function = GroupFunction::Ocurence,
          bool SimdHash = true,
          unsigned int NPerGroup = 4,
          typename F = decltype(&default_groupfn_impl)>
requires GroupFn<F, first_arg_grp_t<F>>
void transform_group_by_sametype_mt(const std::vector<unsigned int>& x,
                           const n_col int = -1,
                           const std::string colname = "n",
                           F f = &default_groupfn_impl) 
{

    if (in_view) {
        std::cerr << "Can't use this operation while in `view` mode, " 
                  << "consider applying `.materialize()`\n";
        return;
    }

    if constexpr (Function != GroupFunction::Occurence) {
        if (n_col < 0) {
            std::cerr << "Can't take negative columns\n";
            return;
        } else if (n_col > ncol) {
            std::cerr << "Column number out of range\n";
            return;
        }
    }

    using value_t = std::conditional_t<(Function == GroupFunction::Occurence), 
                                  unsigned int,
                                  std::condistional_t<
                                  !(std::is_same_v<TColVal, void>),
                                  std::conditional_t<Function == GroupFunction::Gather,
                                                     ReservingVec<element_type_t<TColVal>>,
                                                     element_type_t<TColVal>>,
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
    
    key_variant_t key_table  = nullptr;
    key_variant_t key_table2 = nullptr;
  
    if constexpr (!std::is_same_v<TContainer, void>) {
        if constexpr (std::is_same_v<TContainer, std::string>) {
            key_table = &str_v;
            idx_type = 0;
        } else if constexpr (std::is_same_v<TContainer, CharT>) {
            key_table = &chr_v;
            idx_type = 1;
        } else if constexpr (std::is_same_v<TContainer, uint8_t>) {
            key_table = &bool_v;
            idx_type = 2;
        } else if constexpr (std::is_same_v<TContainer, IntT>) {
            key_table = &int_v;
            idx_type = 3;
        } else if constexpr (std::is_same_v<TContainer, UIntT>) {
            key_table = &uint_v;
            idx_type = 4;
        } else if constexpr (std::is_same_v<TContainer, FloatT>) {
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

    size_t n_col_real;
    if constexpr (Function != GroupFunction::Occurence) {
        switch (type_refv[n_col]) {
            case 's': key_table2 = &str_v;  idx_type = 0; break;
            case 'c': key_table2 = &chr_v;  idx_type = 1; break;
            case 'b': key_table2 = &bool_v; idx_type = 2; break;
            case 'i': key_table2 = &int_v;  idx_type = 3; break;
            case 'u': key_table2 = &uint_v; idx_type = 4; break;
            case 'd': key_table2 = &dbl_v;  idx_type = 5; break;
        }
        if constexpr (Function == GroupFunction::Gather) {
            using R = std::remove_cvref_t<
                std::invoke_result_t<F, std::vector<TContainer>&>
            >;
            if constexpr (std::is_same_v<R, std::string>) {
                idx_type = 0;
            } else if constexpr (std::is_same_v<R, CharT>) {
                idx_type = 1;
            } else if constexpr (std::is_same_v<R, uint8_t>) {
                idx_type = 2;
            } else if constexpr (std::is_same_v<R, IntT>) {
                idx_type = 3;
            } else if constexpr (std::is_same_v<R, UIntT>) {
                idx_type = 4;
            } else if constexpr (std::is_same_v<R, FloatT>) {
                idx_type = 5;
            } else {
                static_assert(always_false<F>, "Unsupported type F");
            }
        }
        auto it = std::find(matr_idx[idx_type].begin(), matr_idx[idx_type].end(), n_col);
        if (it != matr_idx[idx_type].end()) {
            n_col_real = std::distance(matr_idx[idx_type].begin(), it);
            break;
        }
    } else {
        idx_vec = 4;
    }

    value_t zero;
    value_t vec;

    if constexpr (std::is_same_v<TColVal, void>) {
        zero = make_zero(idx_type);
        vec  = make_vec(idx_type, NPerGroup);
    }

    map_t lookup;
    lookup.reserve(local_nrow);
    std::vector<std::string*> key_vec(local_nrow);

    auto key_build = [&](std::string& key, unsigned int i) {
       for (size_t j = 0; j < x.size(); ++j) {
           if constexpr (!std::is_same_v<TContainer, std::string>) {
               if constexpr (std::is_same_v<TContainer, CharT>) {
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
                if constexpr (std::is_same_v<TColVal, void>) {
                    auto [it, inserted] = lookup.try_emplace(key, zero);
                    ++(it->second);
                } else {
                    auto [it, inserted] = lookup.try_emplace(key, 0);
                    ++(it->second);
                }
            } else if constexpr (Function == GroupFunction::Sum ||
                                 Function == GroupFunction::Mean) {
                if constexpr (std::is_same_v<TColVal, void>) {
                    auto [it, inserted] = lookup.try_emplace(key, zero);
                    (it->second) += (*key_table2)[n_col_real][i];
                } else {
                    auto [it, inserted] = lookup.try_emplace(key, 0);
                    (it->second) += (*key_table2)[n_col_real][i];
                }
            } else {
                if constexpr (std::is_same_v<TColVal, void>) {
                    auto [it, inserted] = lookup.try_emplace(key, vec);
                    it->second.push_back((*key_table2)[n_col_real][i]);
                } else {
                    auto [it, inserted] = lookup.try_emplace(key, NPerGroup);
                    it->second.push_back((*key_table2)[n_col_real][i]);
                }
            } 
            key_vec[i] = &it->first;
        }
    } else if constexpr (CORES > 1) {
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
                    if constexpr (std::is_same_v<TColVal, void>) {
                        auto [it, inserted] = cur_map.try_emplace(key, zero);
                        ++(it->second);
                    } else {
                        auto [it, inserted] = cur_map.try_emplace(key, 0);
                        ++(it->second);
                    }
                } else if constexpr (Function == GroupFunction::Sum ||
                                     Function == GroupFunction::Mean) {
                    if constexpr (std::is_same_v<TColVal, void>) {
                        auto [it, inserted] = cur_map.try_emplace(key, zero);
                        (it->second) += (*key_table2)[n_col_real][i];
                    } else {
                        auto [it, inserted] = cur_map.try_emplace(key, 0);
                        (it->second) += (*key_table2)[n_col_real][i];
                    }
                } else {
                    if constexpr (std::is_same_v<TColVal, void>) {
                        auto [it, inserted] = cur_map.try_emplace(key, vec);
                        it->second.push_back((*key_table2)[n_col_real][i]);
                    } else {
                        auto [it, inserted] = cur_map.try_emplace(key, NPerGroup);
                        it->second.push_back((*key_table2)[n_col_real][i]);
                    }
                }
            }
        }
        for (const auto& cur_map : vec_map) {
            for (const auto& [k, v] : cur_map) {
                if constexpr (Function == GroupFunction::Occurence ||
                              Function == GroupFunction::Sum ||
                              Function == GroupFunction::Mean) {
                    if constexpr (std::is_same_v<TColVal, void>) {
                        auto [it, inserted] = lookup.try_emplace(k, zero);
                        (it->second) += v;
                    } else {
                        auto [it, inserted] = lookup.try_emplace(k, 0);
                        (it->second) += v;
                    }
                } else {
                    if constexpr (std::is_same_v<TColVal, void>) {
                        auto [it, inserted] = lookup.try_emplace(k, vec);
                        const unsigned int n_old_size = it->second.size();
                        it->second.resize(n_old_size + v.size());
                        if (triv_copy) {
                            memcpy(it->second.data() + n_old_size,
                                   v.data(),
                                   sizeof(TColVal) * v.size());
                        } else {
                            it->second.insert(it->second.begin() + n_old_size,
                                              v.begin(),
                                              v.end());
                        }
                    } else {
                        auto [it, inserted] = lookup.try_emplace(k, NPerGroup);
                        const unsigned int n_old_size = it->second.size();
                        it->second.resize(n_old_size + v.size());
                        if (std::is_trivially_copyable<TColVal>) {
                            memcpy(it->second.data() + n_old_size,
                                   v.data(),
                                   sizeof(TColVal) * v.size());
                        } else {
                            it->second.insert(it->second.begin() + n_old_size,
                                              v.begin(),
                                              v.end());
                        }
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

    value_t value_col = make_vec(idx_type);
    value_col.resize(local_nrow);
    #pragma omp parallel for if(CORES > 1) num_threads(CORES)
    for (size_t i = 0; i < key_vec.size(); ++i) {
        unsigned int count;
        if constexpr (Function == GroupFunction::Occurence || 
                      Function == GroupFunction::Sum) {
            count = lookup.at(*key_vec[i]);
        } else if constexpr (Function == GroupFunction::Mean) {
            count = lookup.at(*key_vec[i]) / local_nrow;
        } else if constexpr (Function == GroupFunction::Gather) {
            count = f(lookup.at(*key_vec[i]));
        }
        value_col[i] = count;
    }
   
    switch (idx_type) {
        case 0: type_refv.push_back('s'); str_v.push_back(value_col);  break;
        case 1: type_refv.push_back('c'); chr_v.push_back(value_col);  break;
        case 2: type_refv.push_back('b'); bool_v.push_back(value_col); break;
        case 3: type_refv.push_back('i'); int_v.push_back(value_col);  break;
        case 4: type_refv.push_back('u'); uint_v.push_back(value_col); break;
        case 5: type_refv.push_back('d'); dbl_v.push_back(value_col);  break;
    }

    if (!name_v.empty())
        name_v.push_back(colname);

    ++ncol;
}


