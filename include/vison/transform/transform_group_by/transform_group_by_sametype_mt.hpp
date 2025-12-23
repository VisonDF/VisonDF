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
                                    const n_col int,
                                    const std::string colname = "n",
                                    F f = &default_groupfn_impl) 
{

    if constexpr (Function != GroupFunction::Occurence) {
        if (n_col > ncol) {
            std::cerr << "Column number out of range\n";
            return;
        }
    }

    unsigned int I = 0;
    for (auto& el : grp_by_col) {
        if (contains_all(el, x)) {
            transform_group_by_hard_alrd_mt<CORES, 
                                            NPerGroup>(I, n, colname);
            return;
        }
        I += 1;
    }

    using col_value_t = std::conditional_t<Function == GroupFunction::Occurence, 
                                           std::vector<UIntT>,
                                           std::conditional_t<!(std::is_same_v<TColVal, void>),
                                                              std::vector<element_type_t<TColVal>>,
                                           std::variant<
                                                 std::vector<std::string>, 
                                                 std::vector<CharT>, 
                                                 std::vector<uint8_t>, 
                                                 std::vector<IntT>, 
                                                 std::vector<UIntT>, 
                                                 std::vector<FloatT>
                                                 >>>;
    using map_t = std::conditional_t<
        SimdHash,
    std::conditional_t<Function == GroupFunction::Occurence,
                           ankerl::unordered_dense::map<key_t, UIntT, simd_hash>,   
                           std::variant<
                                   ankerl::unordered_dense::map<key_t, std::string>,             simd_hash>, 
                                   ankerl::unordered_dense::map<key_t, CharT>,                   simd_hash>, 
                                   ankerl::unordered_dense::map<key_t, uint8_t>,                 simd_hash>, 
                                   ankerl::unordered_dense::map<key_t, IntT>,                    simd_hash>, 
                                   ankerl::unordered_dense::map<key_t, UIntT>,                   simd_hash>, 
                                   ankerl::unordered_dense::map<key_t, FloatT>,                  simd_hash>,
                                   ankerl::unordered_dense::map<key_t, ReservingVec<std::string>, simd_hash>, 
                                   ankerl::unordered_dense::map<key_t, ReservingVec<CharT>,       simd_hash>, 
                                   ankerl::unordered_dense::map<key_t, ReservingVec<uint8_t>,     simd_hash>, 
                                   ankerl::unordered_dense::map<key_t, ReservingVec<IntT>,        simd_hash>, 
                                   ankerl::unordered_dense::map<key_t, ReservingVec<UIntT>,       simd_hash>, 
                                   ankerl::unordered_dense::map<key_t, ReservingVec<FloatT>,      simd_hash>
                       >

               >,
    std::conditional_t<Function == GroupFunction::Occurence,
                           ankerl::unordered_dense::map<key_t, UIntT>,  
                           std::variant<
                                   ankerl::unordered_dense::map<key_t, std::string>, 
                                   ankerl::unordered_dense::map<key_t, CharT>, 
                                   ankerl::unordered_dense::map<key_t, uint8_t>, 
                                   ankerl::unordered_dense::map<key_t, IntT>, 
                                   ankerl::unordered_dense::map<key_t, UIntT>, 
                                   ankerl::unordered_dense::map<key_t, FloatT>,
                                   ankerl::unordered_dense::map<key_t, ReservingVec<std::string>>, 
                                   ankerl::unordered_dense::map<key_t, ReservingVec<CharT>>, 
                                   ankerl::unordered_dense::map<key_t, ReservingVec<uint8_t>>, 
                                   ankerl::unordered_dense::map<key_t, ReservingVec<IntT>>, 
                                   ankerl::unordered_dense::map<key_t, ReservingVec<UIntT>>, 
                                   ankerl::unordered_dense::map<key_t, ReservingVec<FloatT>>
                       >

               >
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
        auto it = std::find(matr_idx[idx_type].begin(), matr_idx[idx_type].end(), n_col);
        if (it != matr_idx[idx_type].end()) {
            n_col_real = std::distance(matr_idx[idx_type].begin(), it);
            break;
        } else {
            std::err << "`TColVal` type missmatch\n";
            return;
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
    } else {
        idx_type = 4;
    }

    map_t lookup;
    if constexpr (std::is_same_v<TColVal; void>) {
        if constexpr (Function != GroupFunction::Gather) {
            lookup.emplace<idx_type>();
        } else {
            lookup.emplace<idx_type + 6>();
        }
    }
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

    auto dispatch_from_void = [&](auto&& f, 
                                  std::string& key, 
                                  size_t start, 
                                  size_t end, 
                                  map_t& cmap) {
        std::visit([&](auto&& tbl_ptr) {
            using TP = std::remove_cvref_t<decltype(tbl_ptr)>;    
            if constexpr (!std::is_same_v<TP, std::nullptr_t>) {
                auto const& val_col = (*tbl_ptr)[n_col_real]; 
                using Elem = typename std::decay_t<decltype(val_col)>::value_type;
                if constexpr (Function == GroupFunction::Occurence) {
                    Elem zero = 0;
                    f(key, start, end, cmap, zero);
                } else if constexpr (Function != GroupFunction::Gather) {
                    Elem zero = 0;
                    f(val_col, key, start, end, cmap, zero);
                } else {
                    ReservingVec<Elem> vec(NPerGroup);
                    f(val_col, key, start, end, cmap, vec);
                }
            }
        }, key_table2);
    };

    auto occ_lookup = [&](std::string& key, 
                          size_t start, 
                          size_t end, 
                          map_t& cmap,
                          const auto& zero) {
        for (unsigned int i = start; i < end; ++i) {
            key.clear();
            key_build(key, i);
            auto [it, inserted] = cmap.try_emplace(key, zero);
            ++it->second;
            key_vec[i] = &it->first;
        }
    };

    auto add_lookup = [&](const auto& val_col, 
                          std::string& key, 
                          size_t start, 
                          size_t end, 
                          map_t& cmap,
                          const auto& zero) {
        for (unsigned int i = start; i < end; ++i) {
            key.clear();
            key_build(key, i);
            auto [it, inserted] = cmap.try_emplace(key, zero);
            (it->second) += val_col[i];
            key_vec[i] = &it->first;
        }
    };

    auto fill_lookup = [&](const auto& val_col, 
                           std::string& key, 
                           size_t start, 
                           size_t end, 
                           map_t& cmap,
                           const auto& vec) {
        for (unsigned int i = start; i < end; ++i) {
            key.clear();
            key_build(key, i);
            auto [it, inserted] = cmap.try_emplace(key, vec);
            it->second.push_back(val_col[i]);
            key_vec[i] = &it->first;
        }
    };

    if constexpr (CORES == 1) {
        std::string key;
        key.reserve(2048);
        if constexpr (Function == GroupFunction::Occurence) {
            dispatch_from_void(occ_lookup, key, 0, local_nrow, lookup);
        } else if constexpr (Function == GroupFunction::Sum ||
                             Function == GroupFunction::Mean) {
            dispatch_from_void(add_lookup, key, 0, local_nrow, lookup);
        } else {
            dispatch_from_void(fill_lookup, key, 0, local_nrow, lookup);
        }
    } else if constexpr (CORES > 1) {
        const bool triv_copy = (idx_type != 0);
        const unsigned int chunks = local_nrow / CORES + 1;
        constexpr auto& size_table = get_types_size();
        const size_t val_size = size_table[idx_type];
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
            if constexpr (Function == GroupFunction::Occurence) {
                dispatch_from_void(occ_lookup, key, start, end, cur_map);
            } else if constexpr (Function == GroupFunction::Sum ||
                     Function == GroupFunction::Mean) {
                dispatch_from_void(add_lookup, key, start, end, cur_map);
            } else {
                dispatch_from_void(fill_lookup, key, start, end, cur_map);
            }
        }
        if (triv_copy) {
            std::visit([&](auto&& tbl_ptr) {
                using TP = std::remove_cvref_t<decltype(tbl_ptr)>;    
                if constexpr (!std::is_same_v<TP, std::nullptr_t>) {
                    auto const& val_col = (*tbl_ptr)[n_col_real]; 
                    using Elem = typename std::decay_t<decltype(val_col)>::value_type;
                    Elem zero = 0;
                    ReservingVec<Elem> vec(NPerGroup);
                    for (const auto& cur_map : vec_map) {
                        for (const auto& [k, v] : cur_map) {
                            if constexpr (Function == GroupFunction::Occurence ||
                                          Function == GroupFunction::Sum ||
                                          Function == GroupFunction::Mean) {
                                auto [it, inserted] = lookup.try_emplace(k, zero);
                                (it->second) += v;
                            } else {
                                auto [it, inserted] = lookup.try_emplace(k, vec);
                                const unsigned int n_old_size = it->second.size();
                                memcpy(it->second.data() + n_old_size,
                                       v.data(),
                                       val_size * v.size());
                            }
                        }
                    }
                }
            }
            , key_table2);
        } else {
            std::visit([&](auto&& tbl_ptr) {
                using TP = std::remove_cvref_t<decltype(tbl_ptr)>;    
                if constexpr (!std::is_same_v<TP, std::nullptr_t>) {
                    auto const& val_col = (*tbl_ptr)[n_col_real]; 
                    using Elem = typename std::decay_t<decltype(val_col)>::value_type;
                    Elem zero = 0;
                    ReservingVec<Elem> vec(NPerGroup);
                    for (const auto& cur_map : vec_map) {
                        for (const auto& [k, v] : cur_map) {
                            if constexpr (Function == GroupFunction::Occurence ||
                                          Function == GroupFunction::Sum ||
                                          Function == GroupFunction::Mean) {
                                auto [it, inserted] = lookup.try_emplace(k, zero);
                                (it->second) += v;
                            } else {
                                auto [it, inserted] = lookup.try_emplace(k, vec);
                                it->second.resize(n_old_size + v.size());
                                it->second.insert(it->second.end(),
                                                  v.begin(),
                                                  v.end());
                            }
                        }
                    }
                }
            }
            , key_table2);
        }
        #pragma omp parallel for num_threads(CORES)
        for (size_t i = 0; i < local_nrow; ++i) {
            std::string key;
            key.reserve(2048);
            key_build(key, i);
            key_vec[i] = &lookup.find(key)->first;
        }
    }

    col_value_t value_col;
    if constexpr (std::is_same_v<TColVal, void> && Function != GroupFunction::Occurence) {
        value_col.emplace<idx_type>();  
    }
    value_col.resize(local_nrow);
    #pragma omp parallel for if(CORES > 1) num_threads(CORES)
    for (size_t i = 0; i < key_vec.size(); ++i) {
        if constexpr (Function == GroupFunction::Occurence || 
                      Function == GroupFunction::Sum) {
            value_col[i] = lookup.at(*key_vec[i]);
        } else if constexpr (Function == GroupFunction::Mean) {
            value_col[i] = lookup.at(*key_vec[i]) / local_nrow;
        } else if constexpr (Function == GroupFunction::Gather) {
            value_col[i] = f(lookup.at(*key_vec[i]));
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

    if (!name_v.empty())
        name_v.push_back(colname);

    ++ncol;
}


