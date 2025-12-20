#pragma once

template <typename TContainer = void,
          typename TColVal = void,
          unsigned int CORES = 4,
          GroupFunction Function = GroupFunction::Occurence,
          bool SimdHash = true,
          unsigned int NPerGroup = 4,
          typename F = decltype(&default_groupfn_impl)>
requires GroupFn<F, first_arg_grp_t<F>>
void transform_group_by_onecol_hard_mt(unsigned int x,
                                       const n_col int,
                                       const std::string colname = "n",
                                       F f = &default_groupfn_impl) 
{

    if (in_view) {
        std::cerr << "Can't use this operation while in `view` mode, " 
                  << "consider applying `.materialize()`\n";
        return;
    }

    if constexpr (!Occurence) {
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
    using col_value_t = std::conditional_t<Occurence, 
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
                           std::conditional_t<
                                !(std::is_same_v<TColVal, void>),
                                std::conditional_t<Function == GroupFunction::Gather,
                                    ankerl::unordered_dense::map<key_t, PairGroupBy<ReservingVec<element_type_t<TColVal>>>, simd_hash>,
                                    ankerl::unordered_dense::map<key_t, PairGroupBy<element_type_t<TColVal>>>, 		    simd_hash>
				>,
                                std::variant<
                                        ankerl::unordered_dense::map<PairGroupBy<std::string>, 		     simd_hash>, 
                                        ankerl::unordered_dense::map<PairGroupBy<CharT>,       		     simd_hash>, 
                                        ankerl::unordered_dense::map<PairGroupBy<uint8_t>,     		     simd_hash>, 
                                        ankerl::unordered_dense::map<PairGroupBy<IntT>,        		     simd_hash>, 
                                        ankerl::unordered_dense::map<PairGroupBy<UIntT>,       		     simd_hash>, 
                                        ankerl::unordered_dense::map<PairGroupBy<FloatT>,      		     simd_hash>,
                                        ankerl::unordered_dense::map<PairGroupBy<ReservingVec<std::string>>, simd_hash>, 
                                        ankerl::unordered_dense::map<PairGroupBy<ReservingVec<CharT>>,       simd_hash>, 
                                        ankerl::unordered_dense::map<PairGroupBy<ReservingVec<uint8_t>>,     simd_hash>, 
                                        ankerl::unordered_dense::map<PairGroupBy<ReservingVec<IntT>>,        simd_hash>, 
                                        ankerl::unordered_dense::map<PairGroupBy<ReservingVec<UIntT>>,       simd_hash>, 
                                        ankerl::unordered_dense::map<PairGroupBy<ReservingVec<FloatT>>,      simd_hash>
                                >

			  >,
	std::conditional_t<Function == GroupFunction::Occurence,
                           std::conditional_t<
                                !(std::is_same_v<TColVal, void>),
                                std::conditional_t<Function == GroupFunction::Gather,
                                    ankerl::unordered_dense::map<key_t, PairGroupBy<ReservingVec<element_type_t<TColVal>>>>,
                                    ankerl::unordered_dense::map<key_t, PairGroupBy<element_type_t<TColVal>>>,            >
				>,
                                std::variant<
                                        ankerl::unordered_dense::map<PairGroupBy<std::string>, 		     >, 
                                        ankerl::unordered_dense::map<PairGroupBy<CharT>,       		     >, 
                                        ankerl::unordered_dense::map<PairGroupBy<uint8_t>,     		     >, 
                                        ankerl::unordered_dense::map<PairGroupBy<IntT>,        		     >, 
                                        ankerl::unordered_dense::map<PairGroupBy<UIntT>,       		     >, 
                                        ankerl::unordered_dense::map<PairGroupBy<FloatT>,      		     >,
                                        ankerl::unordered_dense::map<PairGroupBy<ReservingVec<std::string>>, >, 
                                        ankerl::unordered_dense::map<PairGroupBy<ReservingVec<CharT>>,       >, 
                                        ankerl::unordered_dense::map<PairGroupBy<ReservingVec<uint8_t>>,     >, 
                                        ankerl::unordered_dense::map<PairGroupBy<ReservingVec<IntT>>,        >, 
                                        ankerl::unordered_dense::map<PairGroupBy<ReservingVec<UIntT>>,       >, 
                                        ankerl::unordered_dense::map<PairGroupBy<ReservingVec<FloatT>>,      >
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
    if constexpr (std::is_same_v<TColVal, void>) {
	if constexpr (Function != GroupFunction::Gather) {
            lookup.emplace<idx_type>();
	} else {
            lookup.emplace<idx_type + 6>();
	}
    }
    lookup.reserve(local_nrow);

    size_t n_col_real;
    if constexpr (!Occurence) {
        switch (type_refv[x]) {
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

    const auto& key_col = (*key_table)[real_pos];

    auto dispatch_from_void = [&](auto&& f, size_t start, size_t end, map_t& cmap) {
        std::visit([&](auto&& tbl_ptr) {
            using TP = std::remove_cvref_t<decltype(tbl_ptr)>;
    
            if constexpr (!std::is_same_v<TP, std::nullptr_t>) {
                auto const& val_col = (*tbl_ptr)[n_col_real]; 
                using Elem = typename std::decay_t<decltype(val_col)>::value_type;
		if constexpr (Function == GroupFunction::Occurence) {
                    PairGroupBy<Elem> vec_struct(NPerGRoup);
                    f(start, end, cmap, vec_struct);
		} if constexpr (Function != GroupFunction::Gather) {
                    PairGroupBy<Elem> vec_struct(NPerGRoup);
                    f(val_col, start, end, cmap, vec_struct);
                } else {
                    PairGroupBy<ReservingVec<Elem>> vec_struct(NPerGRoup);
                    f(val_col, start, end, cmap, vec_struct);
                }
            }
        }, key_table2);
    };

    auto occ_lookup = [&](size_t start, 
		          size_t end, 
			  map_t& cmap,
			  const auto& vec_struct) {
        for (unsigned int i = start; i < end; ++i) {
            auto [it, inserted] = cmap.try_emplace(key_col[i], vec_struct);
            auto& cur_struct = it->second;
            ++cur_struct.value;
            cur_struct.idx_vec.push_back(i);
        }
    };

    auto add_lookup = [&](const auto& val_col, 
		          size_t start, 
			  size_t end, 
			  map_t& cmap,
			  const auto& vec_struct) {
        for (unsigned int i = start; i < end; ++i) {
            auto [it, inserted] = cmap.try_emplace(key_col[i], vec_struct);
            auto& cur_struct = it->second;
            cur_struct.value += val_col[i];
            cur_struct.idx_vec.push_back(i);
        }
    };

    auto fill_lookup = [&](const auto& val_col, 
		           size_t start, 
			   size_t end, 
			   map_t& cmap,
			   const auto& vec_struct) {
        for (unsigned int i = start; i < end; ++i) {
             auto [it, inserted] = cmap.try_emplace(key_col[i], vec_struct);
             auto& cur_struct = it->second;
             cur_struct.value.push_back(val_col[i]);
             cur_struct.idx_vec.push_back(i);
        }
    };

    if constexpr (CORES == 1) {
        if constexpr (Function == GroupFunction::Occurence) {
	    dispatch_from_void(occ_lookup, 0, local_nrow, lookup);
        } else if constexpr (Function == GroupFunction::Sum ||
                             Function == GroupFunction::Mean) {
	    dispatch_from_void(add_lookup, 0, local_nrow, lookup);
        } else {
	    dispatch_from_void(fill_lookup, 0, local_nrow, lookup);
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
            if constexpr (Function == GroupFunction::Occurence) {
	        occ_lookup(start, end, cur_map);
            } else if constexpr (Function == GroupFunction::Sum ||
                                 Function == GroupFunction::Mean) {
	        dispatch_from_void(add_lookup, start, end, cur_map);
            } else {
	        dispatch_from_void(fill_lookup, start, end, cur_map);
            }
        }
        if (triv_copy) {
            std::visit([&](auto&& tbl_ptr) {
                using TP = std::remove_cvref_t<decltype(tbl_ptr)>;
                if constexpr (!std::is_same_v<TP, std::nullptr_t>) {
                    auto const& val_col = (*tbl_ptr)[n_col_real]; 
                    using Elem = typename std::decay_t<decltype(val_col)>::value_type;
		    PairGroupBy<Elem> zero_struct(NPerGroup);
		    PairGroupBy<ReservingVec<Elem>> zero_struct(NPerGroup);
                    for (const auto& cur_map : vec_map) {
                        for (const auto& [k, v] : cur_map) {
                            if constexpr (Function == GroupFunction::Occurence ||
                                          Function == GroupFunction::Mean ||
                                          Function == GroupFunction::Sum) {
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
                                const unsigned int n_old_size_val = cur_struct.size();
                                cur_struct.value.resize(n_old_size_val + v.size());
                                memcpy(cur_struct.value.data() + n_old_size_val,
                                       v.value.data(),
                                       v.value.size() * val_size);
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
	    }
	    , key_table2);
        } else {
            std::visit([&](auto&& tbl_ptr) {
                using TP = std::remove_cvref_t<decltype(tbl_ptr)>;
                if constexpr (!std::is_same_v<TP, std::nullptr_t>) {
                    auto const& val_col = (*tbl_ptr)[n_col_real]; 
                    using Elem = typename std::decay_t<decltype(val_col)>::value_type;
		    PairGroupBy<Elem> zero_struct(NPerGroup);
		    PairGroupBy<ReservingVec<Elem>> zero_struct(NPerGroup);
                    for (const auto& cur_map : vec_map) {
                        for (const auto& [k, v] : cur_map) {
                            if constexpr (Function == GroupFunction::Occurence ||
                                          Function == GroupFunction::Mean ||
                                          Function == GroupFunction::Sum) {
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
                                cur_struct.value.insert(cur_struct.value.end(),
                                                        v.value.begin(),
                                                        v.value.end());
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
	    }
	    , key_table2);
        }
    }

    col_value_t value_col;
    if constexpr (std::is_same_v<TColVal, void>) {
	value_col.emplace<idx_type>();
    }
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
	    if constexpr (Function == GroupFunction::Occurence ||
			  Function == GroupFunction::Sum) {
		for (size_t t = 0; t < vec.size(); ++t)
                    value_col[start + t] = cur_val;
	    } else if constexpr (Function == GroupFunction::Mean) {
		for (size_t t = 0; t < vec.size(); ++t)
                    value_col[start + t] = cur_val / local_nrow;
	    } else {
		for (size_t t = 0; t < vec.size(); ++t)
                    value_col[start + t] = f(cur_val);
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
	    if constexpr (Function == GroupFunction::Occurence ||
			  Function == GroupFunction::Sum) {
		for (size_t t = 0; t < pos_vec.size(); ++t)
                    value_col[i2 + t] = cur_val;
	    } else if constexpr (Function == GroupFunction::Mean) {
		for (size_t t = 0; t < pos_vec.size(); ++t)
                    value_col[i2 + t] = cur_val / local_nrow;
	    } else {
		for (size_t t = 0; t < pos_vec.size(); ++t)
                    value_col[i2 + t] = f(cur_val);
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

    ++ncol;
}


