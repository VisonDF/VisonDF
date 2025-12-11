#pragma once

template <unsigned int CORES = 4,
          bool Occurence = false,
          bool SimdHash = true>
void transform_group_by_onecol_hard_mt(unsigned int x,
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

    using key_t = std::conditional_t<!std::is_same_v<T, void>, 
                                     T, 
                                     std::variant<std::string, 
                                        CharT, 
                                        uint8_t, 
                                        IntT, 
                                        UIntT, 
                                        FloatT>>;
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
        ankerl::unordered_dense::map<key_t, 
                                     PairGroupBy<value_t>, 
                                     simd_hash>,
        ankerl::unordered_dense::map<key_t, 
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
    key_variant_t key_table2 = nullptr;
    
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

    if constexpr (CORES == 1) {
        for (unsigned int i = 0; i < local_nrow; ++i) {    
            auto [it, inserted] = lookup.try_emplace((*key_table)[real_pos][i], 0);
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
            const unsigned int tid   = omp_get_thread_num();
            const unsigned int start = tid * chunks;
            const unsigned int end   = std::min(local_nrow, start + chunks);
            map_t& cur_map           = vec_map[tid];
            cur_map.reserve(local_nrow / CORES);
            for (size_t i = start; i < end; ++i) {
                auto [it, inserted] = cur_map.try_emplace((*key_table)[real_pos][i], 0);
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
            key_vec[i] = &lookup.find((*key_table)[real_pos][i])->first;
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


