#pragma once

template <typename T, bool MemClean = false, bool IsBool = false>
void get_col_filter_range(unsigned int &x,
                          std::vector<T> &rtn_v,
                          const std::vector<uint8_t> &mask,
                          const unsigned int &strt_vl)
{
    rtn_v.reserve(nrow);

    auto extract_range_masked =
    [&](const auto *__restrict col_ptr, const auto &idx_vec)
    {
        size_t pos = 0;
        while (pos < idx_vec.size() && idx_vec[pos] != x)
            ++pos;

        if (pos == idx_vec.size()) {
            std::cerr << "Error in (get_col), no column found\n";
            return;     
        }

        const size_t base = pos * nrow;
        const auto *__restrict src = col_ptr + base;

        for (size_t i = 0; i < nrow; ++i) {
            if (mask[i]) {
                rtn_v.push_back(src[strt_vl + i]);
            }
        }
    };

    if constexpr (IsBool) {
        extract_range_masked(bool_v.data(), matr_idx[2]);

    } else if constexpr (std::is_same_v<T, IntT>) {
        extract_range_masked(int_v.data(), matr_idx[3]);

    } else if constexpr (std::is_same_v<T, UIntT>) {
        extract_range_masked(uint_v.data(), matr_idx[4]);

    } else if constexpr (std::is_same_v<T, FloatT>) {
        extract_range_masked(dbl_v.data(), matr_idx[5]);

    } else if constexpr (std::is_same_v<T, std::string>) {
        extract_range_masked(str_v.data(), matr_idx[0]);

    } else if constexpr (std::is_same_v<T, CharT>) {
        extract_range_masked(chr_v.data(), matr_idx[1]);

    } else {
        std::cerr << "Error in (get_col), unsupported type\n";
        return;
    }

    if constexpr (MemClean)
        rtn_v.shrink_to_fit();
}



