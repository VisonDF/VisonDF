#pragma once

template <typename T                    = void,
           bool MapCol                  = false,
           bool MemClean                = false,
           AssertionType AssertionLevel = AssertionType::Simple
         >
void rm_col(unsigned int nbcol) {

    static_assert(is_supported_type<element_type_t<T>>, "Type not supported");

    const unsigned int local_ncol = ncol;
    if constexpr (AssertionLevel > AssertionType::None) {
        if (nbcol > local_ncol) {
            throw std::runtime_error("Column number out of bound\n");
        }
    }

    size_t idx_in_type;
    size_t idx_type;

    using container_t = std::conditional_t<std::is_same_v<T, void>,
                            std::variant<std::monostate,
                             std::vector<std::string>*,
                             std::vector<CharT>*,
                             std::vector<uint8_t>*,
                             std::vector<IntT>*,
                             std::vector<UIntT>*,
                             std::vector<FloatT>*
                            >,
                            std::vector<element_type_t<T>>*
    >;

    key_t var_container_table;

    if constexpr (!std::is_same_v<T, void>) {

        if constexpr (std::is_same_v<element_type_t<T>, std::string>) {
            idx_type = 0;
            var_container_table = str_v;
        } else if constexpr (std::is_same_v<element_type_t<T>, CharT>) {
            idx_type = 1;
            var_container_table = chr_v;
        } else if constexpr (std::is_same_v<element_type_t<T>, uint8_t>) {
            idx_type = 2;
            var_container_table = bool_v;
        } else if constexpr (std::is_same_v<element_type_t<T>, IntT>) {
            idx_type = 3;
            var_container_table = int_v;
        } else if constexpr (std::is_same_v<element_type_t<T>, UIntT>) {
            idx_type = 4;
            var_container_table = uint_v;
        } else if constexpr (std::is_same_v<element_type_t<T>, FloatT>) {
            idx_type = 5;
            var_container_table = dbl_v;
        } else {
            std::cerr << "Impossible type\n";
            return;
        }

    } else {

        switch (type_refv[nbcol]) {
            case 's': idx_type = 0; break;
            case 'c': idx_type = 1; break;
            case 'b': idx_type = 2; break;
            case 'i': idx_type = 3; break;
            case 'u': idx_type = 4; break;
            case 'd': idx_type = 5; break;
            default: throw std::runtime_error("Type unknown, can not exist\n");
        }

    }

    if constexpr (!MapCol) {

        idx_in_type =
            std::find(matr_idx[idx_type].begin(), matr_idx[idx_type].end(), nbcol)
            - matr_idx[idx_type].begin();

    } else {

        if (!matr_idx_map[idx_type].contains(nbcol)) {
            std::cerr << "MapCol chosen, but no corresponding col found in the map\n";
            return;
        }

        if (!sync_map_col[idx_type]) {
            std::cerr << "MapCol not synced\n";
            return;
        }

        idx_type = matr_idx_map[idx_type][nbcol];

    }

    if (!name_v.empty()) { name_v.erase(name_v.begin() + nbcol); }
    type_refv.erase(type_refv.begin() + nbcol);

    matr_idx[idx_type].erase(matr_idx[idx_type].begin() + idx_in_type);
    matr_idx_map[idx_type].erase(nbcol);

    if constexpr (std::is_same_v<T, void>) {

        std::visit([idx_type](auto&& vec) {

            vec.erase(key_table.begin()  + idx_in_type); 

        }, var_container_table);

    } else {

        var_container_table.erase(var_container_table.begin()  + idx_in_type); 

    }

    for (auto& [k, v] : matr_idx_map[idx_type]) {
        if (v > idx_in_type)
            v -= 1;
    }
    for (size_t i = 0; i < 6; ++i) {
        for (auto& el : matr_idx[i]) {
            if (el > nbcol)
                el -= 1;
        }
        for (auto& [k, v] : matr_idx_map[i]) {
            if (k > nbcol)
                k -= 1;
        }
    }

    if constexpr (MemClean) {
        name_v                .shrink_to_fit();
        type_refv             .shrink_to_fit();
        matr_idx[idx_type]    .shrink_to_fit();
        matr_idx_map[idx_type].shrink_to_fit();

        if constexpr (std::is_same_v<T, void>) {

            std::visit([idx_type](auto&& vec) {
                vec.shrink_to_fit(); 
            }, var_container_table);

        } else {

            var_container_table.shrink_to_fit();

        }
    }

    ncol -= 1;
}






