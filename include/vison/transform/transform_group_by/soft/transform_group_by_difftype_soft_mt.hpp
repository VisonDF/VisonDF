#pragma once

template <typename TContainer = void,
      unsigned int CORES = 4,
          bool SimdHash = true,
      unsigned int NPerGroup = 4,
      bool SanityCheck = true>
void transform_group_by_difftype_soft_mt(const std::vector<unsigned int>& x,
                                         const std::string colname = "n") 
{

    if constexpr (SanityCheck) {
        unsigned int I = 0;
        for (auto& el : x) {
            if (std::sort(el.begin(), el.end()) == x) {
                transform_group_by_soft_alrd_mt<I, 
                                                CORES, 
                                                NPerGroup, 
                                                SanityCheck>(x, colname);
                return;
            }
            I += 1;
        }
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
    ReservingVec midx_vec<unsigned int>(NPerGroup);

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
        std::string key;
        key.reserve(2048);
        for (unsigned int i = 0; i < local_nrow; ++i) { 
            key.clear();
            key_build(key, i);
            auto [it, inserted] = lookup.try_emplace(key, midx_vec);
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
            for (unsigned int i = start; i < end; ++i) {
                key.clear();
                key_build(key, i);
                auto [it, inserted] = cur_map.try_emplace(key, midx_vec);
                it->second.push_back(i);
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
            const group_vec_t& vec = (it0 + g)->second.idx_vec;
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
            const auto& vec = (it + i)->second.idx_vec;
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
    
    if (!name_v.empty())
        name_v.push_back(colname);

    type_refv.push_back('u');
    ++ncol;
}


