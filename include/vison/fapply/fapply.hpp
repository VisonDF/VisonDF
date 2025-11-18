#pragma once

template <typename F>
requires FapplyFn<F, first_arg_t<F>>
void fapply(F f, unsigned int& n) {

    using T = first_arg_t<F>;

    if constexpr (std::is_same_v<T, bool>)
        apply_numeric<decltype(bool_v), bool>(bool_v, n, 0, f);

    else if constexpr (std::is_same_v<T, IntT>)
        apply_numeric<decltype(int_v), IntT>(int_v, n, 3, f);

    else if constexpr (std::is_same_v<T, UIntT>)
        apply_numeric<decltype(uint_v), UIntT>(uint_v, n, 4, f);

    else if constexpr (std::is_same_v<T, FloatT>)
        apply_numeric<decltype(dbl_v), FloatT>(dbl_v, n, 5, f);

    else if constexpr (std::is_same_v<T, char>) {
        unsigned int i2 = 0;
        while (i2 < matr_idx[1].size() && n != matr_idx[1][i2])
            ++i2;
        const unsigned int start = nrow * i2;
        unsigned int i3 = 0;
        for (size_t i = start; i < start + nrow; ++i, ++i3) {
            f(chr_v[i]);
            tmp_val_refv[n][i3].assign(1, chr_v[i]);
        }
    }

    else if constexpr (std::is_same_v<T, std::string>) {
        unsigned int i2 = 0;
        while (i2 < matr_idx[0].size() && n != matr_idx[0][i2])
            ++i2;
        const unsigned int start = nrow * i2;
        unsigned int i3 = 0;
        for (size_t i = start; i < start + nrow; ++i, ++i3) {
            f(str_v[i]);
            tmp_val_refv[n][i3] = str_v[i];
        }
    }
}

