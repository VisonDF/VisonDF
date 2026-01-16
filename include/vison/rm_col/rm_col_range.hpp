#pragma once

template <typename T                    = void,
           bool MapCol                  = false,
           bool MemClean                = false,
           bool SortedDesc              = true,  // you can lie here
           AssertionType AssertionLevel = AssertionType::Simple
         >
void rm_col_range(std::vector<unsigned int>& nbcol) {

    static_assert(is_supported_type<element_type_t<T>>, "Type not supported");

    const unsigned int local_ncol = ncol;
    if constexpr (AssertionLevel > AssertionType::None) {
        for (auto el : nbcol) {
            if (el > local_ncol) {
                throw std::runtime_error("Column number out of bound\n");
            }
        }
    }

    if constexpr (!SortedDesc) {
        std::sort(nbcol.begin(), nbcol.end());
        std::reverse(nbcol.begin(), nbcol.end());
    }

    size_t idx_in_type;
    size_t idx_type;

    std::variant<std::monostate,
                 std::vector::std::vector<std::string>&,
                 std::vector::std::vector<CharT>&,
                 std::vector::std::vector<uint8_t>&,
                 std::vector::std::vector<IntT>&,
                 std::vector::std::vector<UIntT>&,
                 std::vector::std::vector<FloatT>&,
                 > var_key_table;
    ankerl::unordered_dense::map<char, unsigned int> map_idx_col;

    if constexpr (!std::is_same_v<T, void>) {

        if constexpr (std::is_same_v<element_type_t<T>, std::string>) {
            idx_type = 0;
            var_key_table = str_v;
        } else if constexpr (std::is_same_v<element_type_t<T>, CharT>) {
            idx_type = 1;
            var_key_table = chr_v;
        } else if constexpr (std::is_same_v<element_type_t<T>, uint8_t>) {
            idx_type = 2;
            var_key_table = bool_v;
        } else if constexpr (std::is_same_v<element_type_t<T>, IntT>) {
            idx_type = 3;
            var_key_table = int_v;
        } else if constexpr (std::is_same_v<element_type_t<T>, UIntT>) {
            idx_type = 4;
            var_key_table = uint_v;
        } else if constexpr (std::is_same_v<element_type_t<T>, FloatT>) {
            idx_type = 5;
            var_key_table = dbl_v;
        } else {
            std::cerr << "Impossible type\n";
            return;
        }

    } else {
        map_idx_col.emplace('s', 0);
        map_idx_col.emplace('c', 1);
        map_idx_col.emplace('b', 2);
        map_idx_col.emplace('i', 3);
        map_idx_col.emplace('u', 4);
        map_idx_col.emplace('d', 5);
    }

    bool alrd = false;

    for (auto cur_nbcol : nbcol) {


        if constexpr (!MapCol) {

            idx_type = map_idx_col[cur_nbcol];

            idx_in_type =
                std::find(matr_idx[idx_type].begin(), matr_idx[idx_type].end(), cur_nbcol)
                - matr_idx[idx_type].begin();

        } else {

            if (!matr_idx_map[idx_type].contains(cur_nbcol)) {
                std::cerr << "MapCol chosen, but no corresponding col found in the map\n";
                return;
            }

            if (!sync_map_col[idx_type] || alrd) {
                std::cerr << "MapCol not synced\n";
                return;
            }

            alrd = true;

            idx_type = matr_idx_map[idx_type][cur_nbcol];

        }

        if (!name_v.empty()) { name_v.erase(name_v.begin() + cur_nbcol); }
        type_refv.erase(type_refv.begin() + cur_nbcol);
        matr_idx[idx_type].erase(matr_idx[type_i].begin() + idx_in_type);
        matr_idx_map[idx_type].erase(cur_nbcol);

        if constexpr (!std::is_same_v<T, void>) {

            std::visit([idx_type](auto&& key_table) {
                key_table.erase(key_table.begin()  + idx_in_type); 
            }, var_key_table);

        } else {

            switch (type_i) {
                case 0: {
                            str_v.erase(str_v.begin()  + idx_in_type); 
                            break;
                        }
                case 1: {
                            chr_v.erase(chr_v.begin()  + idx_in_type); 
                            break;
                        }
                case 2: {
                            bool_v.erase(bool_v.begin() + idx_in_type); 
                            break;
                        }
                case 3: {
                            int_v.erase(int_v.begin()  + idx_in_type); 
                            break;
                        }
                case 4: {
                            uint_v.erase(uint_v.begin() + idx_in_type); 
                            break;
                        }
                case 5: {
                            dbl_v.erase(dbl_v.begin()  + idx_in_type); 
                            break;
                        }
            }

            for (auto& [k, v] : matr_idx_map[idx_type]) {
                if (v > idx_in_type)
                    v -= 1;
            }

        }

    }

    for (auto cur_nbcol : nbcol) {

        for (size_t i = 0; i < 6; ++i) {
            for (auto& el : matr_idx[i]) {
                if (el > cur_nbcol)
                    el -= 1;
            }

            for (auto& [k, v] : matr_idx_map[i]) {
                if (k > cur_nbcol)
                    k -= 1;
            }

        }

    }

    if constexpr (!std::is_same_v<T, void>) {

        name_v                .shrink_to_fit();
        type_refv             .shrink_to_fit();
        matr_idx[idx_type]    .shrink_to_fit();
        matr_idx_map[idx_type].shrink_to_fit();

        std::visit([idx_type](auto&& key_table) {
            key_table.shrink_to_fit(); 
        }, var_key_table);

    } else {

        if constexpr (MemClean) {
            name_v   .shrink_to_fit();
            type_refv.shrink_to_fit();

            for (auto& el : matr_idx)
                el.shrink_to_fit();

            for (auto& el : matr_idx_map)
                el.shrink_to_fit();

            str_v .shrink_to_fit();
            chr_v .shrink_to_fit();
            bool_v.shrink_to_fit();
            int_v .shrink_to_fit();
            uint_v.shrink_to_fit();
            dbl_v .shrink_to_fit();
        }

    }

    ncol -= nbcol.size();
}






