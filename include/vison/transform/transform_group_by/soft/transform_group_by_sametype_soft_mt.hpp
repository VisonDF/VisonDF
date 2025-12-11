#pragma once

template <unsigned int CORES = 4,
          bool SimdHash = true>
void transform_group_by_sametype_soft_mt(const std::vector<unsigned int>& x,
                                         const std::string colname = "n") 
{

    if (in_view) {
        std::cerr << "Can't use this operation while in `view` mode, " 
                  << "consider applying `.materialize()`\n";
        return;
    }

    using map_t = std::conditional_t<
        SimdHash,
        ankerl::unordered_dense::map<std::string, 
                                     std::vector<unsigned int>, 
                                     simd_hash>,
        ankerl::unordered_dense::map<std::string, 
                                     std::vector<unsigned int>>
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

    map_t lookup;
    lookup.reserve(local_nrow);
    
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
            auto [it, inserted] = lookup.try_emplace(key, 0);
            it->second.push_back(i);
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
                it->second.push_back(i);
            }
        }
        for (const auto& cur_map : vec_map) {
            for (const auto& [k, v] : cur_map) {
                auto [it, inserted] = lookup.try_emplace(k, 0);
                const unsigned int n_old_size = it->second.size();
                it->second.resize(n_old_size + v.size());
                memcpy(it->second.data() + n_old_size,
                       v.data(),
                       v.size() * sizeof(T)
                       );
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
            memcpy(row_view_idx.data() + start,
                   vec.data(),
                   len * sizeof(unsigned int));
        }
    } else {
        auto it = lookup.begin();
        size_t i2 = 0;
        for (size_t i = 0; i < lookup.size(); ++i) {
            const auto& pos_vec = (it + i)->second.idx_vec;
            memcpy(row_view_idx.data() + i2, 
                   pos_vec.data(), 
                   sizeof(unsigned int) * pos_vec.size());
            i2 += pos_vec.size();
        }
    }

    if (!name_v.empty())
        name_v.push_back(colname);

    type_refv.push_back('u');
    ++ncol;
}


