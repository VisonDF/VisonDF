#pragma once

template <typename TContainer  = void,
          typename TColVal = void,
          unsigned int CORES = 4,
          GroupFunction Function = GroupFunction::Occurence,
          bool SimdHash = true,
          unsigned int NPerGroup = 4,
          typename F = decltype(&default_groupfn_impl)>
requires GroupFn<F, first_arg_grp_t<F>>
void transform_group_by_onecol_mt(const unsigned int x,
                                  const n_col unsigned int,
                                  const std::string colname = "n",
                                  const F f = &default_groupfn_impl) 
{

    if (in_view) {
        std::cerr << "Can't use this operation while in `view` mode, " 
                  << "consider applying `.materialize()`\n";
        return;
    }

    if constexpr (Function != GroupFunction::Occurence) {
        if (n_col > ncol) {
            std::cerr << "Column number out of range\n";
            return;
        }
    }

    using key_t = std::conditional_t<!std::is_same_v<TContainer, void>, 
                                     TContainer, 
                                     std::variant<std::string, 
                                        CharT, 
                                        uint8_t, 
                                        IntT, 
                                        UIntT, 
                                        FloatT>>;
    using value_t = std::conditional_t<(Function == GroupFunction::Occurence), 
                                  unsigned int,
                                  std::conditional_t<
                                  !(std::is_same_v<TColVal, void>),
                                  std::conditional_t<Function == GroupFunction::Gather,
                                                     ReservingVec<element_type_t<TColVal>>,
                                                     element_type_t<TColVal>>,
                                  std::variant<
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
        ankerl::unordered_dense::map<key_t, 
                                     value_t, 
                                     simd_hash>,
        ankerl::unordered_dense::map<key_t, 
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
        switch (type_refv[x]) {
            case 's': key_table = &str_v;  idx_type = 0; break;
            case 'c': key_table = &chr_v;  idx_type = 1; break;
            case 'b': key_table = &bool_v; idx_type = 2; break;
            case 'i': key_table = &int_v;  idx_type = 3; break;
            case 'u': key_table = &uint_v; idx_type = 4; break;
            case 'd': key_table = &dbl_v;  idx_type = 5; break;
        }
    }

    std::unordered_map<int, int> pos;
    for (int i = 0; i < matr_idx[idx_type].size(); ++i)
        pos[matr_idx[idx_type][i]] = i;
    const size_t real_pos = pos[x];

    map_t lookup;
    lookup.reserve(local_nrow);

    std::vector<key_t*> key_vec(local_nrow);

    size_t n_col_real;
    if constexpr (Function != GroupFunction::Occurence) {
        switch (type_refv[x]) {
            case 's': key_table2 = &str_v;  idx_type = 0; break;
            case 'c': key_table2 = &chr_v;  idx_type = 1; break;
            case 'b': key_table2 = &bool_v; idx_type = 2; break;
            case 'i': key_table2 = &int_v;  idx_type = 3; break;
            case 'u': key_table2 = &uint_v; idx_type = 4; break;
            case 'd': key_table2 = &dbl_v;  idx_type = 5; break;
            default:
                std::abort();
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

    constexpr value_t zero = make_zero<value_t>(idx_type);
    constexpr value_t vec  = make_vec<value_t>(idx_type, NPerGroup);

    if constexpr (CORES == 1) {
        for (unsigned int i = 0; i < local_nrow; ++i) {
            if constexpr (Function == GroupFunction::Occurence) {
                auto [it, inserted] = lookup.try_emplace((*key_table)[real_pos][i], 
                                                         zero);
                ++(it->second);
            } else if constexpr (Function == GroupFunction::Sum 
                                 || Function == GroupFunction::Mean) {
                auto [it, inserted] = lookup.try_emplace((*key_table)[real_pos][i], 
                                                         zero);
                (it->second) += (*key_table2)[n_col_real][i];
            } else {
                auto [it, inserted] = lookup.try_emplace((*key_table)[real_pos][i], 
                                                          vec);
                it->second.push_back((*key_table2)[n_col_real][i]);
            }
            key_vec[i] = &it->first;
        }
    } else if constexpr (CORES > 1) {
        constexpr auto& size_table = get_types_size();
        const size_t val_size = size_table[idx_type];
        const bool triv_copy = (idx_type != 0);
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
                if constexpr (Function == GroupFunction::Occurence) {
                   auto [it, inserted] = cur_map.try_emplace((*key_table)[real_pos][i], 
                                                             zero);
                   ++(it->second);
                } else if constexpr (Function == GroupFunction::Sum
                                     || Function == GroupFunction::Mean) {
                    auto [it, inserted] = cur_map.try_emplace((*key_table)[real_pos][i], 
                                                              zero);
                    (it->second) += (*key_table2)[n_col_real][i];
                } else {
                    auto [it, inserted] = lookup.try_emplace((*key_table)[real_pos][i], 
                                                              vec);
                    it->second.push_back((*key_table2)[n_col_real][i]);
                }
            }
        }
        if (is_triv) {
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
                        it->second.resize(n_old_size + v.size());
                        memcpy(it->second.data() + n_old_size, 
                               v.data(), 
                               val_size * v.size());
                    }
                }
            }
        } else {
            for (const auto& cur_map : vec_map) {
                for (const auto& [k, v] : cur_map) {
                    if constexpr (Function == GroupFunction::Occurence ||
                                  Function == GroupFunction::Sum ||
                                  Function == GroupFunction::Mean) {
                        auto [it, inserted] = lookup.try_emplace(k, zero);
                        (it->second) += v;
                    } else {
                        auto [it, inserted] = lookup.try_emplace(k, vec);
                        it->second.insert(it->second.end(),
                                          v.begin(),
                                          v.end());
                    }
                }
            }
        }
        #pragma omp parallel for num_threads(CORES)
        for (size_t i = 0; i < local_nrow; ++i)
            key_vec[i] = &lookup.find((*key_table)[real_pos][i])->first;
    }

    value_t value_col = make_vec<value_t>(idx_type, 0);
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




