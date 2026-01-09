#pragma once

template <typename T = void,
          unsigned int CORES = 4,
          bool MemClean = false, 
          bool Last = false,
          bool MapCol = false,
          bool Soft = true,
          bool SimdHash = true>
void transform_unique_mt(unsigned int n) 
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

    auto aply_filter = [val_size,
                        &mask,
                        &lookup]<typename Elem>(const auto& key_col) {

        if constexpr (!Last) {
            if constexpr (std::is_same_v<Elem, std::string>) {
                 for (size_t i = 0; i < local_nrow; ++i) {
                     if (!lookup.contains(key_col[i])) {
                         mask[i] = 0;
                         lookup.emplace(key_col[i]);
                     }
                 }
            } else {
                for (size_t i = 0; i < local_nrow; ++i) {
                    if (!lookup.contains(std::string_view{reinterpret_cast<const char*>(&key_col[i]), val_size})) {
                        mask[i] = 0;
                        lookup.emplace(std::string_view{reinterpret_cast<const char*>(&key_col[i]), val_size});
                    }
                }
            }
        } else {
            if constexpr (std::is_same_v<Elem, std::string>) {
                for (int i = int(local_nrow) - 1; i >= 0; --i) {
                    if (!lookup.contains(key_col[i])) [[unlikely]] {
                        mask[i] = 0;
                        lookup.emplace(key_col[i]);
                    }
                }
            } else {
                for (int i = int(local_nrow) - 1; i >= 0; --i) {
                    if (!lookup.contains(std::string_view{reinterpret_cast<const char*>(&key_col[i]), val_size})) [[unlikely]] {
                        mask[i] = 0;
                        lookup.emplace(std::string_view{reinterpret_cast<const char*>(&key_col[i]), val_size});
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

                const auto& key_col = (*key_table)[real_pos];

                apply_filter<Elem>(key_col);

            }

        }, var_key_table);

    } else {

        const auto& key_col = (*var_key_table)[real_pos];

        apply_filter<element_type_t<T>>(key_col);

    }

    this->transform_filter_mt<CORES,
                              MemClean, 
                              false, //SmallProportion
                              Soft>(mask);

};










