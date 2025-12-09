#pragma once

template <typename T = void,
          unsigned int CORES = 4, 
          bool MemClean = false, 
          bool SimdHash = true,
          bool Soft = true,
          bool Inner = false>
inline void transform_inner_excluding(Dataframe &cur_obj, 
                                      unsigned int in_col, 
                                      unsigned int ext_col) 
{

    auto& type_refv2 = cur_obj.get_typecol();
    unsigned int& ncol2 = cur_obj.get_ncol();
    if (in_col > ncol) {
        std::cerr << "col key outside of bound\n";
        return;
    } else if (ext_col > ncol2) {
        std::cerr << "col key outside of bound for outside dataframe\n";
        return;
    } else if (type_refv[in_col] != type_refv2[ext_col]) {
        std::cerr << "Can't perform an excluding transformation with different col type\n";
        return;
    }
 
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

    unsigned int idx_type;
    if constexpr (!std::is_same_v<T, void>) {
        if constexpr (std::is_same_v<T, std::string>) {
            key_table = &str_v;
            key_table2 = &cur_obj.get_str_v();
            idx_type = 0;
        } else if constexpr (std::is_same_v<T, CharT>) {
            key_table = &chr_v;
            key_table2 = &cur_obj.get_chr_v();
            idx_type = 1;
        } else if constexpr (std::is_same_v<T, uint8_t>) {
            key_table = &bool_v;
            key_table2 = &cur_obj.get_bool_v();
            idx_type = 2;
        } else if constexpr (std::is_same_v<T, IntT>) {
            key_table = &int_v;
            key_table2 = &cur_obj.get_int_v();
            idx_type = 3;
        } else if constexpr (std::is_same_v<T, UIntT>) {
            key_table = &uint_v;
            key_table2 = &cur_obj.get_uint_v();
            idx_type = 4;
        } else if constexpr (std::is_same_v<T, FloatT>) {
            key_table = &dbl_v;
            key_table2 = &cur_obj.get_dbl_v();
            idx_type = 5;
        }
    } else { 
        switch (type_refv[type_refv[in_col]]) {
            case 's': key_table = &str_v;  key_table2 = &cur_obj.get_str_v() ; idx_type = 0; break;
            case 'c': key_table = &chr_v;  key_table2 = &cur_obj.get_chr_v() ; idx_type = 1; break;
            case 'b': key_table = &bool_v; key_table2 = &cur_obj.get_bool_v(); idx_type = 2; break;
            case 'i': key_table = &int_v;  key_table2 = &cur_obj.get_int_v() ; idx_type = 3; break;
            case 'u': key_table = &uint_v; key_table2 = &cur_obj.get_uint_v(); idx_type = 4; break;
            case 'd': key_table = &dbl_v;  key_table2 = &cur_obj.get_dbl_v() ; idx_type = 5; break;
        }
    }

    std::unordered_map<unsigned int, unsigned int> pos;
    for (size_t t = 0; t < matr_idx[idx_type].size(); ++t)
        pos[matr_idx[idx_type]] = t;
    auto& in_colv = key_table[pos[in_col]];

    auto& matr_idx2 = cur_obj.get_matr_idx();
    std::unordered_map<unsigned int, unsigned int> pos2;
    for (size_t t = 0; t < matr_idx2[idx_type].size(); ++t)
        pos2[matr_idx2[idx_type]] = t;
    auto& ext_colv = key_table2[pos2[ext_col]];

    const unsigned int& ext_nrow = cur_obj.get_nrow();
    const unsigned int nrow2 = nrow;

    using value_t =  std::variant<std::string*, 
                                  CharT*, 
                                  uint8_t*, 
                                  IntT*, 
                                  UIntT*, 
                                  FloatT*>;

    using fast_set_t = std::conditional_t<
        SimdHash,
        ankerl::unordered_dense::set<value_t, simd_hash>,
        ankerl::unordered_dense::set<value_t>
    >;

    fast_set_t lookup;
    lookup.reserve(ext_nrow);

    for (const auto& el : ext_colv)
        lookup.insert(el);

    std::vector<uint8_t> mask(nrow2);

    #pragma omp parallel for if (CORES > 1) num_threads(CORES) schedule(static)
    for (unsigned int i = 0; i < nrow2; ++i) {
        if constexpr (!Inner) {
            mask[i] = !lookup.contains(in_colv[i]);
        } else {
            mask[i] = lookup.contains(in_colv[i]);
        }
    }

    this->transform_filter_mt<CORES, 
                              MemClean,
                              false, //SmallProportion
                              Soft>(mask);

}



