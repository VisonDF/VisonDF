#pragma once

template <typename T, bool IsBool = false>
void get_col_filter_idx(unsigned int &x,
                        std::vector<T> &rtn_v,
                        const std::vector<unsigned int> &mask)
{
    const size_t n_el = mask.size();
    rtn_v.resize(n_el);

    auto find_col_base = [&](auto &idx_vec) -> size_t {
        size_t pos = 0;
        while (pos < idx_vec.size() && idx_vec[pos] != x)
            ++pos;

        if (pos == idx_vec.size()) {
            std::cerr << "Error in (get_col), no column found\n";
            return size_t(-1);
        }
        return pos;
    };

    auto extract_idx_masked = [&](const auto *__restrict src) {
        for (size_t i = 0; i < n_el; ++i) {
            rtn_v[i] = src[mask[i]];
        }
    };

    if constexpr (IsBool) {
        const size_t pos_base = find_col_base(matr_idx[2]);
        extract_idx_masked(bool_v[pos_base].data());

    } else if constexpr (std::is_same_v<T, IntT>) {
        const size_t pos_base = find_col_base(matr_idx[3]);
        extract_idx_masked(int_v[pos_base].data());

    } else if constexpr (std::is_same_v<T, UIntT>) {
        const size_t pos_base = find_col_base(matr_idx[4]);
        extract_idx_masked(uint_v[pos_base].data());

    } else if constexpr (std::is_same_v<T, FloatT>) {
        const size_t pos_base = find_col_base(matr_idx[5]);
        extract_idx_masked(dbl_v[pos_base].data());

    } else if constexpr (std::is_same_v<T, std::string>) {
        const size_t pos_base = find_col_base(matr_idx[0]);
        extract_idx_masked(str_v[pos_base].data());

    } else if constexpr (std::is_same_v<T, CharT>) {
        const size_t pos_base = find_col_base(matr_idx[1]);
        extract_idx_masked(chr_v[pos_base].data());

    } else {
        std::cerr << "Error in (get_col), unsupported type\n";
        return;
    }
}




