#pragma once

template <typename TContainer = void,
          unsigned int CORES = 4,
          bool SimdHash = true,
          bool MapCol = false,
          unsigned int NPerGroup>
void transform_group_by_onecol_soft_mt(unsigned int x) 
{

    unsigned int I = 0;
    for (auto& el : grp_by_col) {
        if (el.contains(x)) {
            transform_group_by_soft_alrd_mt<CORES, 
                                            NPerGroup>(I);
            return;
        }
        I += 1;
    }

    using map_t = std::conditional_t<
        SimdHash,
        ankerl::unordered_dense::set<std::string_view, 
                                     ReservingVec<unsigned int>, 
                                     simd_hash>,
        ankerl::unordered_dense::set<std::string_view, 
                                     ReservingVec<unsigned int>>
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

    size_t real_pos;
    if constexpr (!MapCol) {
        const auto& cur_matr_idx = matr_idx[idx_type];
        for (int i = 0; i < cur_matr_idx.size(); ++i) {
            if (cur_matr_idx[i] == x) {
                real_pos = i;
                break;
            }
        }
        real_pos = pos[x];
    } else {
        real_pos = matr_idx_map[idx_type][x];
    }

    map_t lookup;
    lookup.reserve(local_nrow / NPerGroup);
    ReservingVec midx_vec<unsigned int>(NPerGroup);
    auto& key_col = (*key_table)[real_pos];

    if constexpr (CORES == 1) {
        if constexpr (std::is_same_v<TContainer, std::string>) {
            for (unsigned int i = 0; i < local_nrow; ++i) {    
                auto [it, inserted] = lookup.try_emplace(key_col[i], midx_vec);
                it->second.push_back(i);
            }
        } else {
            constexpr auto& size_table = get_types_size();
            const unsigned int val_size = size_table[idx_type];
            if (idx_type != 0) {
                for (unsigned int i = 0; i < local_nrow; ++i) {    
                    auto [it, inserted] = lookup.try_emplace(std::string_view{reinterpret_cast<const char*>(key_col[i]), 
                                                             val_size}, 
                                                             midx_vec);
                    it->second.push_back(row_view_idx[i]);
                }
            } else {
                for (unsigned int i = 0; i < local_nrow; ++i) {    
                    auto [it, inserted] = lookup.try_emplace(key_col[i], midx_vec);
                    it->second.push_back(row_view_idx[i]);
                }
            }
        }
    } else if constexpr (CORES > 1) {
        const unsigned int chunks = local_nrow / CORES + 1;
        std::vector<map_t> vec_map(CORES);
        #pragma omp parallel num_threads(CORES)
        {
            const unsigned int rsv_val = local_nrow / (CORES * NPerGroup);
            const unsigned int tid   = omp_get_thread_num();
            const unsigned int start = tid * chunks;
            const unsigned int end   = std::min(local_nrow, start + chunks);
            map_t& cur_map           = vec_map[tid];
            cur_map.reserve(rsv_val);
            if constexpr (std::is_same_v<TContainer, std::string>) {
                for (unsigned int i = start; i < end; ++i) {
                    auto [it, inserted] = cur_map.try_emplace(key_col[i], midx_vec);
                    it->second.push_back(i);
                }
            } else {
                if (idx_type != 0) {
                    for (unsigned int i = start; i < end; ++i) {
                        auto [it, inserted] = cur_map.try_emplace(std::string_view{reinterpret_cast<const char*>(key_col[i]), 
                                                                  val_size}, 
                                                                  midx_vec);
                        it->second.push_back(row_view_idx[i]);
                    }
                } else {
                    for (unsigned int i = start; i < end; ++i) {
                        auto [it, inserted] = cur_map.try_emplace(key_col[i], midx_vec);
                        it->second.push_back(i);
                    }
                }
            }
        }
        for (const auto& cur_map : vec_map) {
            for (const auto& [k, v] : cur_map) {
                auto [it, inserted] = lookup.try_emplace(k, midx_vec);
                const unsigned int n_old_size = it->second.size();
                it->second.resize(n_old_size + v.size());
                memcpy(it->second.data() + n_old_size,
                       v.data(),
                       v.size() * sizeof(unsigned int)
                       );
            }
        }
    }

    if (!in_view) {
        in_view = true;
        row_view_idx.resize(local_nrow);
        row_view_map.reserve(local_nrow);
        for (size_t i = 0; i < local_nrow; ++i)
            row_view_map.emplace(i, i);
    }

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
            const group_vec_t& vec = (it0 + g)->second;
            for (auto& el : vec)
                el = row_view_map[el];
            memcpy(row_view_idx.data() + start,
                   vec.data(),
                   len * sizeof(unsigned int));
        }
    } else {
        auto it = lookup.begin();
        size_t i2 = 0;
        for (size_t i = 0; i < lookup.size(); ++i) {
            const auto& vec = (it + i)->second;
            for (auto& el : vec)
                el = row_view_map[el];
            memcpy(row_view_idx.data() + i2, 
                   vec.data(), 
                   sizeof(unsigned int) * vec.size());
            i2 += vec.size();
        }
    }

    for (size_t i = 0; i < local_nrow; ++i)
        row_view_map[i] = row_view_idx[i];

}


