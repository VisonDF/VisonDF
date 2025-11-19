#pragma once

template <typename T, bool MemClean = false, bool IsBool = false>
void get_col_filter(unsigned int &x,
                    std::vector<T> &rtn_v,
                    const std::vector<uint8_t> &mask)
{
    rtn_v.reserve(nrow);

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

    auto extract_masked = [&](const auto *__restrict col_ptr, size_t base) {
        const auto *src = col_ptr + base;
        size_t idx = 0;

        for (size_t i = 0; i < nrow; ++i, ++idx) {
            if (mask[i])
                rtn_v.push_back(src[idx]);
        }
    };

    if constexpr (IsBool) {
        extract_masked(bool_v.data(),
                       find_col_base(matr_idx[2]));

    } else if constexpr (std::is_same_v<T, IntT>) {
        extract_masked(int_v.data(),
                       find_col_base(matr_idx[3]));

    } else if constexpr (std::is_same_v<T, UIntT>) {
        extract_masked(uint_v.data(),
                       find_col_base(matr_idx[4]));

    } else if constexpr (std::is_same_v<T, FloatT>) {
        extract_masked(dbl_v.data(),
                       find_col_base(matr_idx[5]));

    } else if constexpr (std::is_same_v<T, std::string>) {
        extract_masked(str_v.data(),
                       find_col_base(matr_idx[0]));

    } else if constexpr (std::is_same_v<T, CharT>) {
        extract_masked(chr_v.data(),
                       find_col_base(matr_idx[1]));

    } else {
        std::cerr << "Error in (get_col), unsupported type\n";
        return;
    }

    if constexpr (MemClean)
        rtn_v.shrink_to_fit();
}



