#pragma once

template <typename T, 
          bool MemClean = false, 
          bool IsBool = false>
inline void get_col_filter_boolmask(unsigned int &x,
                          std::vector<T> &rtn_v,
                          const std::vector<uint8_t> &mask,
                          const unsigned int strt_vl,
                          const unsigned int nrow)
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
        return pos;
    };

    auto extract_masked = [&](const auto *__restrict col_ptr) {
        const auto *src = col_ptr;
        for (size_t i = 0; i < mask.size(); ++i) {
            if (mask[i])
                rtn_v.push_back(src[strt_vl + i]);
        }
    };

    if constexpr (std::is_same_v<T, std::string>) {
        const size_t pos_base = find_col_base(matr_idx[0]);
        extract_masked(str_v[pos_base].data());

    } else if constexpr (std::is_same_v<T, CharT>) {
        const size_t pos_base = find_col_base(matr_idx[1]);
        extract_masked(chr_v[pos_base].data());

    } else if constexpr (IsBool) {
        const size_t pos_base = find_col_base(matr_idx[2]);
        extract_masked(bool_v[pos_base].data());

    } else if constexpr (std::is_same_v<T, IntT>) {
        const size_t pos_base = find_col_base(matr_idx[3]);
        extract_masked(int_v[pos_base].data());

    } else if constexpr (std::is_same_v<T, UIntT>) {
        const size_t pos_base = find_col_base(matr_idx[4]);
        extract_masked(uint_v[pos_base].data());

    } else if constexpr (std::is_same_v<T, FloatT>) {
        const size_t pos_base = find_col_base(matr_idx[5]);
        extract_masked(dbl_v[pos_base].data());

    } else {
        std::cerr << "Error in (get_col), unsupported type\n";
        return;
    }

    if constexpr (MemClean)
        rtn_v.shrink_to_fit();
}

