#pragma once

template <bool IsBool = false,
          bool MapCol = false,
          typename T>
void get_col_filter_idx(unsigned int x,
                        std::vector<T> &rtn_v,
                        const std::vector<unsigned int> &mask)
{
    rtn_v.resize(mask.size());

    auto find_col_base = [x](const auto &idx_vec, [[maybe_unused]] const size_t idx_type) -> size_t {
        size_t pos;
        if constexpr (!MapCol) {
            pos = 0;
            while (pos < idx_vec.size() && idx_vec[pos] != x)
                ++pos;

            if (pos == idx_vec.size()) {
                throw std::runtime_error("Error in (get_col), no column found\n");
            }

        } else {
            if (!matr_idx_map[idx_type].contains(x)) {
                throw std::runtime_error("MapCol chosen but col not found in map\n");
            }
            if (!sync_map_col[idx_type]) {
                throw std::runtime_error("Map not synced\n");
            }
            pos = matr_idx_map[idx_type][x];
        }
        return pos;
    };

    auto extract_idx_masked = [&rtn_v, &mask](const auto*__restrict src) {
        for (auto i : mask)
            rtn_v[i] = src[i];
    };

    if constexpr (std::is_same_v<T, std::string>) {

        const size_t pos_base = find_col_base(matr_idx[0], 0);
        extract_idx_masked(str_v[pos_base].data());

    } else if constexpr (std::is_same_v<T, CharT>) {

        const size_t pos_base = find_col_base(matr_idx[1], 1);
        extract_idx_masked(chr_v[pos_base].data());

    } else if constexpr (IsBool) {

        const size_t pos_base = find_col_base(matr_idx[2], 2);
        extract_idx_masked(bool_v[pos_base].data());

    } else if constexpr (std::is_same_v<T, IntT>) {

        const size_t pos_base = find_col_base(matr_idx[3], 3);
        extract_idx_masked(int_v[pos_base].data());

    } else if constexpr (std::is_same_v<T, UIntT>) {

        const size_t pos_base = find_col_base(matr_idx[4], 4);
        extract_idx_masked(uint_v[pos_base].data());

    } else if constexpr (std::is_same_v<T, FloatT>) {

        const size_t pos_base = find_col_base(matr_idx[5], 5);
        extract_idx_masked(dbl_v[pos_base].data());

    } else {
        std::cerr << "Error in (get_col), unsupported type\n";
        return;
    }
}




