#pragma once

template <typename T,
          unsigned int CORES = 4,
          bool Occurence = false,
          bool SimdHash = true>
void transform_group_by_mt(const std::vector<unsigned int>& x,
                           const n_col int = -1,
                           const std::string colname = "n") 
{

    if constexpr (Occurence) {
        if (n_col < 0) {
            std::cerr << "Can't take negative columns\n";
            return;
        } else if (n_col > ncol) {
            std::cerr << "Column number out of range\n";
            return;
        }
    }

    using map_t = std::conditional_t<
        SimdHash,
        ankerl::unordered_dense::map<T, unsigned int, simd_hash>,
        ankerl::unordered_dense::map<T, unsigned int>
    >;

    const unsigned int local_nrow = nrow;
    std::vector<unsigned int> x2(x.size());
    std::unordered_map<int, int> pos;
    size_t idx_type;

    const std::vector<std:vector<T>>* key_table = nullptr;
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

    for (int i = 0; i < matr_idx[idx_type]; ++i)
        pos[matr_idx[idx_type][i]] = i;
    for (int v : x)
        idx.push_back(pos[v]);

    map_t lookup;
    lookup.reserve(local_nrow);

    std::vector<const T*> key_vec(local_nrow);

    std::string key;
    key.reserve(256);  
    
    for (unsigned int i = 0; i < local_nrow; ++i) {
    
        key.clear();
    
        for (size_t j = 0; j < x.size(); ++j) {
            const auto& src = (*key_table)[x[j]][i];
    
            key.append(src.data(), src.size()); 
            key.push_back('\x1F');              
        }
    
        auto [it, inserted] = lookup.try_emplace(key, 0);
        if constexpr (Occurence) {
            ++(it->second);
        } else if constexpr (!Occurence) {
            (it->second) += (*key_table)[n_col][i];
        }
    
        key_vec[i] = &it->first;
    }

    std::vector<unsigned int> occ_v(local_nrow);

    #pragma omp parallel for if(CORES > 1) num_threads(CORES)
    for (size_t i = 0; i < key_vec.size(); ++i) {
        unsigned int count = lookup.at(*key_vec[i]);
        occ_v[i] = count;
    }
    
    uint_v.insert(uint_v.end(), occ_v.begin(), occ_v.end());

    if (!name_v.empty())
        name_v.push_back(colname);

    type_refv.push_back('u');
    ++ncol;
}


