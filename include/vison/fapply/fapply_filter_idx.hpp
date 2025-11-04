#pragma once

template <typename T>
void fapply_filter_idx(void (&f)(T&), 
                unsigned int& n, 
                const std::vector<unsigned int>& mask) 
{

    if constexpr (std::is_same_v<T, bool>)
        apply_numeric_filter_idx<decltype(bool_v), bool>(bool_v, n, 0, f, mask);

    else if constexpr (std::is_same_v<T, IntT>)
        apply_numeric_filter_idx<decltype(int_v), IntT>(int_v, n, 3, f, mask);

    else if constexpr (std::is_same_v<T, UIntT>)
        apply_numeric_filter_idx<decltype(uint_v), UIntT>(uint_v, n, 4, f, mask);

    else if constexpr (std::is_same_v<T, FloatT>)
        apply_numeric_filter_idx<decltype(dbl_v), FloatT>(dbl_v, n, 5, f, mask);

    else if constexpr (std::is_same_v<T, char>) {
        unsigned int i2 = 0;
        while (i2 < matr_idx[1].size() && n != matr_idx[1][i2])
            ++i2;
        const unsigned int start = nrow * i2;
        unsigned int i3 = 0;
        std::vector<std::string>& val_tmp = tmp_val_refv[n];
        for (; i3 < mask.size(); ++i3) {
            unsigned int pos_idx = mask[i3];
            unsigned int abs_idx = start + mask[i3];
            f(chr_v[abs_idx]);
            val_tmp[pos_idx].assign(1, chr_v[abs_idx]);
        }
    }

    else if constexpr (std::is_same_v<T, std::string>) {
        unsigned int i2 = 0;
        while (i2 < matr_idx[0].size() && n != matr_idx[0][i2])
            ++i2;
        const unsigned int start = nrow * i2;
        unsigned int i3 = 0;
        std::vector<std::string>& val_tmp = tmp_val_refv[n];
        for (; i3 < mask.size(); ++i3) {
            unsigned int pos_idx = mask[i3];
            unsigned int abs_idx = start + mask[i3];
            f(str_v[abs_idx]);
            val_tmp[pos_idx] = str_v[abs_idx];
        }
    }
}



