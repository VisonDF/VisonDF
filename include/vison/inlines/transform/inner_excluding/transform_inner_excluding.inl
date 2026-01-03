#pragma once

template <typename T = void,
          unsigned int CORES = 4, 
          bool MemClean = false, 
          bool SimdHash = true,
          bool Soft = true,
          bool Inner = false,
          bool MapCol = false
    >
inline void transform_inner_excluding(Dataframe &cur_obj, 
                                      unsigned int in_col, 
                                      unsigned int ext_col) 
{

    if (in_view && !Soft) {
        std::cerr << "Can't perform this operation while in `view` mode, consider applying `.materialize()`\n"
        return;
    }

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

    const unsigned int& ext_nrow = cur_obj.get_nrow();
    const unsigned int nrow2 = nrow;

    using fast_set_t = std::conditional_t<
        SimdHash,
        ankerl::unordered_dense::set<std::string_view, simd_hash>,
        ankerl::unordered_dense::set<std::string_view>
    >;
    fast_set_t lookup;
    lookup.reserve(ext_nrow);

    using key_variant_t = std::variant<
        std::monostate,
        const std::vector<std::vector<std::string>>*,
        const std::vector<std::vector<CharT>>*,
        const std::vector<std::vector<uint8_t>>*,
        const std::vector<std::vector<IntT>>*,
        const std::vector<std::vector<UIntT>>*,
        const std::vector<std::vector<FloatT>>*
    >; 
    key_variant_t key_table;
    key_variant_t key_table2;

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

    unsigned int in_idx = 0;
    unsigned int ext_idx = 0;

    if constexpr (!MapCol) {

        std::unordered_map<unsigned int, unsigned int> pos;
        for (size_t t = 0; t < matr_idx[idx_type].size(); ++t)
            pos[matr_idx[idx_type]] = t;
        in_idx = pos[in_col];

        auto& matr_idx2 = cur_obj.get_matr_idx();
        std::unordered_map<unsigned int, unsigned int> pos2;
        for (size_t t = 0; t < matr_idx2[idx_type].size(); ++t)
            pos2[matr_idx2[idx_type]] = t;
        in_idx = pos2[ext_col];

    } else {

        auto& matr_idx_map2 = cur_obj.get_matr_idx_map;

        if (!matr_idx_map[idx_type].contains(in_col)) {
            std::cerr << "MapCol has been chosen, not found in the mapcol\n";
            return;
        }

        if (!sync_map_col[idx_type]) {
            std::cerr << "MapCol is not synced\n";
            return;
        }

        if (!matr_idx_map2[idx_type].contains(ext_col)) {
            std::cerr << "MapCol has been chosen, not found in the mapcol (cur_obj)\n";
            return;
        }

        if (!sync_map_col2[idx_type]) {
            std::cerr << "MapCol is not synced (cur_obj)\n";
            return;
        }

        in_idx  = matr_idx_map[idx_type][in_col];
        ext_idx = matr_idx_map2[idx_type][ext_col];

    }

    std::vector<uint8_t> mask(nrow2);

    std::visit([&mask, in_idx, ext_idx](auto&& tbl_ptr, auto&& tbl_ptr2) {

        using TP  = std::decay_t<decltype(tbl_ptr)>;
        using TP2 = std::decay_t<decltype(tbl_ptr2)>;

        if constexpr (std::is_same_v<TP, TP2>) {

            using Elem = TP::value_type::value_type;

            auto& in_colv  = tbl_ptr[in_idx];
            auto& ext_colv = tbl_ptr2[ext_idx];

            if constexpr (std::is_same_v<Elem, std::string>) {
                for (const auto& el : ext_colv)
                    lookup.insert(el);
            } else {
                constexpr auto& size_table = get_types_size();
                const size_t val_size = size_table[idx_type];
                for (const auto& el : ext_colv) {
                    lookup.insert(std::string_view{reinterpret_cast<const char*>(&el), 
                                  val_size});
                }
            }

            if constexpr (std::is_same_v<Elem, std::string>) {
                if constexpr (!Inner) {
                    #pragma omp parallel for if (CORES > 1) num_threads(CORES) schedule(static)
                    for (unsigned int i = 0; i < nrow2; ++i) {
                        mask[i] = !lookup.contains(in_colv[i]);
                    }
                } else {
                    #pragma omp parallel for if (CORES > 1) num_threads(CORES) schedule(static)
                    for (unsigned int i = 0; i < nrow2; ++i) {
                        mask[i] = lookup.contains(in_colv[i]);
                    }
                }
            } else {
                if constexpr (!Inner) {
                    #pragma omp parallel for if (CORES > 1) num_threads(CORES) schedule(static)
                    for (unsigned int i = 0; i < nrow2; ++i) {
                        mask[i] = !lookup.contains(std::string_view{reinterpret_cast<const char*>(&in_colv[i]), 
                                                                    val_size});
                    }
                } else {
                    #pragma omp parallel for if (CORES > 1) num_threads(CORES) schedule(static)
                    for (unsigned int i = 0; i < nrow2; ++i) {
                        mask[i] = lookup.contains(std::string_view{reinterpret_cast<const char*>(&in_colv[i]), 
                                                                    val_size});
                    }
                }
            }
        }

    }, key_table, key_table2);

    this->transform_filter_mt<CORES, 
                              MemClean,
                              false, //SmallProportion
                              Soft>(mask);

}






