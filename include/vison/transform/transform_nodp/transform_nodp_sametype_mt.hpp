#pragma once

template <typename T = void,
          unsigned int CORES = 4,
          bool MemClean = false, 
          bool Last = false,
          bool MapCol = false,
          bool Soft = true,
          bool SimdHash = true>
void transform_nodp_sametype_mt(std::vector<unsigned int>& n) 
{  

    if (in_view && !Soft) {
        std::cerr << "Can't perform this operation while in `view` mode, consider applying `.materialize()`\n"
        return;
    }

    const size_t local_nrow = nrow;
    std::vector<uint8_t> mask(local_nrow, 1);

    using fast_set_t = std::conditional_t<
        SimdHash,
        ankerl::unordered_dense::set<std::string_view, simd_hash>,
        ankerl::unordered_dense::set<std::string_view>
    >;

    fast_set_t lookup;
    lookup.reserve(local_nrow);

    size_t idx_type;
    using key_variant_t = std::conditional_t<!std::is_same_v<T, void>,
                                             std::vector<std::vector<element_type_t<T>>>*,
                                             std::variant<
                                                 std::monostate,
                                                 const std::vector<std::vector<std::string>>*,
                                                 const std::vector<std::vector<CharT>>*,
                                                 const std::vector<std::vector<uint8_t>>*,
                                                 const std::vector<std::vector<IntT>>*,
                                                 const std::vector<std::vector<UIntT>>*,
                                                 const std::vector<std::vector<FloatT>>*
                                             >
                          >; 
    key_variant_t var_key_table;

    unsigned int idx_type;
    if constexpr (!std::is_same_v<T, void>) {
        if constexpr (std::is_same_v<element_type_t<T>, std::string>) {
            var_key_table = &str_v;
            idx_type = 0;
        } else if constexpr (std::is_same_v<element_type_t<T>, CharT>) {
            var_key_table = &chr_v;
            idx_type = 1;
        } else if constexpr (std::is_same_v<element_type_t<T>, uint8_t>) {
            var_key_table = &bool_v;
            idx_type = 2;
        } else if constexpr (std::is_same_v<element_type_t<T>, IntT>) {
            var_key_table = &int_v;
            idx_type = 3;
        } else if constexpr (std::is_same_v<element_type_t<T>, UIntT>) {
            var_key_table = &uint_v;
            idx_type = 4;
        } else if constexpr (std::is_same_v<element_type_t<T>, FloatT>) {
            var_key_table = &dbl_v;
            idx_type = 5;
        }
    } else { 
        switch (type_refv[type_refv[n]]) {
            case 's': var_key_table = &str_v;  idx_type = 0; break;
            case 'c': var_key_table = &chr_v;  idx_type = 1; break;
            case 'b': var_key_table = &bool_v; idx_type = 2; break;
            case 'i': var_key_table = &int_v;  idx_type = 3; break;
            case 'u': var_key_table = &uint_v; idx_type = 4; break;
            case 'd': var_key_table = &dbl_v;  idx_type = 5; break;
        }
    }

    constexpr auto& size_table = get_types_size();
    const size_t val_size      = size_table[idx_type];

    unsigned int real_pos;

    if constexpr (!MapCol) {

        std::unordered_map<unsigned int, unsigned int> pos;
        for (size_t t = 0; t < matr_idx[idx_type].size(); ++t)
            pos[matr_idx[idx_type]] = t;
 
        real_pos = pos[n];

    } else {

        if (!matr_idx_map[idx_type].contains(n)) {
            std::cerr << "MapCol has been chosen, not found in the mapcol\n";
            return;
        }

        if (!sync_map_col[idx_type]) {
            std::cerr << "MapCol is not synced\n";
            return;
        }

        real_pos = matr_idx_map[idx_type][n]; 
    }

    std::string key_str;
    key_str.reserve(512);

    auto apply_filter = [val_size,
                         &key_str,
                         &mask,
                         &lookup]<typename Elem>(const auto& key_table) {

        if constexpr (!Last) {
            if constexpr (std::is_same_v<Elem, std::string>) {
                 for (size_t i = 0; i < local_nrow; ++i) {
                     key_str.clear();
                     for (auto ncol : n) {
                        key_str += table_col[ncol][i];
                        key_str += "_";
                     }
                     if (!lookup.contains(key_str)) {
                         mask[i] = 0;
                         lookup.emplace(key_str);
                     }
                 }
            } else {
                for (size_t i = 0; i < local_nrow; ++i) {
                     key_str.clear();
                     for (auto ncol : n) {
                        key_str.append(reinterpret_cast<const char*>(&table_col[n][i], sizeof(Elem)));
                        key_str += "_";
                     }
                    if (!lookup.contains(key_str)) {
                        mask[i] = 0;
                        lookup.emplace(key_str);
                    }
                }
            }
        } else {
            if constexpr (std::is_same_v<Elem, std::string>) {
                for (int i = int(local_nrow) - 1; i >= 0; --i) {
                     key_str.clear();
                     for (auto ncol : n) {
                        key_str += table_col[ncol][i];
                        key_str += "_";
                     }
                     if (!lookup.contains(key_str)) {
                         mask[i] = 0;
                         lookup.emplace(key_str);
                     }
                }
            } else {
                for (int i = int(local_nrow) - 1; i >= 0; --i) {
                     key_str.clear();
                     for (auto ncol : n) {
                        key_str.append(reinterpret_cast<const char*>(&table_col[n][i], sizeof(Elem)));
                        key_str += "_";
                     }
                    if (!lookup.contains(key_str)) {
                        mask[i] = 0;
                        lookup.emplace(key_str);
                    }
                }
            }
        }
    };

    if constexpr (std::is_same_v<T, void>) {

        std::visit([real_pos, 
                    val_size, 
                    &lookup, 
                    &mask](auto&& key_table) {

            using TP = std::decay_t<decltype(key_table)>;

            if constexpr (!std::is_same_v<TP, std::monostate>) {

                using Elem = TP::value_type::value_type;

                apply_filter<Elem>(key_table);

            }

        }, var_key_table);

    } else {

        apply_filter<element_type_t<T>>(key_table);

    }

    this->transform_filter_mt<CORES,
                              MemClean, 
                              false, //SmallProportion
                              Soft>(mask);

};










