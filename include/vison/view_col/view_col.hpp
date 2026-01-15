#pragma once

template <typename T, 
          bool MapCol = false>
const std::vector<T>& view_col(unsigned int x) const {

    static_assert(is_supported_type<T>, "Type not supported");

    if (x >= ncol)
        throw std::runtime_error("col number out of bounds\n");

    auto find_col_base = [this, x](const auto& idx_vec, size_t idx_type) -> size_t {

        size_t pos;

        if constexpr (!MapCol) {
            pos = 0;
            while (pos < idx_vec.size() && idx_vec[pos] != x)
                ++pos;
    
            if (pos == idx_vec.size())
                throw std::runtime_error("no column found");
        } else {
            if (!matr_idx_map[idx_type].contains(x))
                throw std::runtime_error("col not found in map");
            if (!sync_map_col[idx_type])
                throw std::runtime_error("map not synced");
    
            pos = matr_idx_map[idx_type][x];
        }
        return pos;
    };

    if constexpr (std::is_same_v<element_type_t<T>, std::string>) {
        size_t idx = find_col_base(matr_idx[0], 0);
        return str_v[idx];
    } else if constexpr (std::is_same_v<element_type_t<T>, CharT>) {
        size_t idx = find_col_base(matr_idx[1], 1);
        return chr_v[idx];
    } else if constexpr (std::is_same_v<element_type_t<T>, uint8_t>) {
        size_t idx = find_col_base(matr_idx[2], 2);
        return bool_v[idx];
    } else if constexpr (std::is_same_v<element_type_t<T>, IntT>) {
        size_t idx = find_col_base(matr_idx[3], 3);
        return int_v[idx];
    } else if constexpr (std::is_same_v<element_type_t<T>, UIntT>) {
        size_t idx = find_col_base(matr_idx[4], 4);
        return uint_v[idx];
    } else if constexpr (std::is_same_v<element_type_t<T>, FloatT>) {
        size_t idx = find_col_base(matr_idx[5], 5);
        return dbl_v[idx];
    } else {
        static_assert(dependent_false<T>, "Unsupported type");
    }

}






