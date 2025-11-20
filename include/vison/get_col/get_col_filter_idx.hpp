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
        return pos * nrow;
    };

    auto extract_idx_masked = [&](const auto *__restrict col_ptr, const auto* idx_vec) {
        const size_t base = find_col_base(idx_vec);
        if (base == size_t(-1)) return;
        const auto *__restrict src = col_ptr + base;
        for (size_t i = 0; i < n_el; ++i) {
            rtn_v[i] = src[mask[i]];
        }
    };

    if constexpr (IsBool) {
        extract_idx_masked(bool_v.data(), matr_idx[2]);

    } else if constexpr (std::is_same_v<T, IntT>) {
        extract_idx_masked(int_v.data(), matr_idx[3]);

    } else if constexpr (std::is_same_v<T, UIntT>) {
        extract_idx_masked(uint_v.data(), matr_idx[4]);

    } else if constexpr (std::is_same_v<T, FloatT>) {
        extract_idx_masked(dbl_v.data(), matr_idx[5]);

    } else if constexpr (std::is_same_v<T, std::string>) {
        extract_idx_masked(str_v.data(), matr_idx[0]);

    } else if constexpr (std::is_same_v<T, CharT>) {
        extract_idx_masked(chr_v.data(), matr_idx[1]);

    } else {
        std::cerr << "Error in (get_col), unsupported type\n";
        return;
    }
}




