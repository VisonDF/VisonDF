#pragma once

template <bool MemClean = false, 
          bool Last = false,
          bool SimdHash = true>
void transform_unique(unsigned int n) 
{  

    const size_t local_nrow = nrow;
    std::vector<uint8_t> mask(local_nrow, 0);
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

    unsigned int idx_type;
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
        switch (type_refv[type_refv[in_col]]) {
            case 's': key_table = &str_v;  idx_type = 0; break;
            case 'c': key_table = &chr_v;  idx_type = 1; break;
            case 'b': key_table = &bool_v; idx_type = 2; break;
            case 'i': key_table = &int_v;  idx_type = 3; break;
            case 'u': key_table = &uint_v; idx_type = 4; break;
            case 'd': key_table = &dbl_v;  idx_type = 5; break;
        }
    }
    std::unordered_map<unsigned int, unsigned int> pos;
    for (size_t t = 0; t < matr_idx[idx_type].size(); ++t)
        pos[matr_idx[idx_type]] = t;
    auto& key_col = key_table[pos[in_col]];

    using key_t =  std::variant<std::string, 
                                  CharT, 
                                  uint8_t, 
                                  IntT, 
                                  UIntT, 
                                  FloatT>;
    
    using fast_set_t = std::conditional_t<
        SimdHash,
        ankerl::unordered_dense::set<key_t, simd_hash>,
        ankerl::unordered_dense::set<key_t>
    >;

    fast_str_set_t lookup;
    lookup.reserve(local_nrow);

    if constexpr (!Last) {
        for (size_t i = 0; i < local_nrow; ++i) {
            if (!lookup.contains(key_col[i])) {
                mask[i] = 1;
                lookup.emplace(key_col[i]);
            }
        }
    } else {
        for (int i = int(local_nrow) - 1; i >= 0; --i) {
            if (!lookup.contains(key_col[i])) [[unlikely]] {
                mask[i] = 1;
                lookup.emplace(key_col[i]);
            }
        }
    }

    this->transform_filter<MemClean>(mask);

};





