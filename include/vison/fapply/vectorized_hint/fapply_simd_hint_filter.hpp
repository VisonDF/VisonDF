#pragma once

template <typename F, bool IsBool = false>
requires FapplyFn<F, first_arg_t<F>>
void fapply_simd_filter(F f, 
                unsigned int& n, 
                const std::vector<uint8_t>& mask) 
{

    assert(mask.size() <= nrow);

    using T = first_arg_t<F>;

    if constexpr (IsBool) {
       
        if constexpr (!(std::is_same_v<T, uint8_t>)) {
          std::cerr << "A bool must be uint8_t\n";
          return;
        }

        apply_numeric_simd_filter<uint8_t>(bool_v, n, 0, f, mask);

    } else if constexpr (std::is_same_v<T, IntT>)
        apply_numeric_simd_filter<IntT>(int_v, n, 3, f, mask);

    else if constexpr (std::is_same_v<T, UIntT>)
        apply_numeric_simd_filter<UIntT>(uint_v, n, 4, f, mask);

    else if constexpr (std::is_same_v<T, FloatT>)
        apply_numeric_simd_filter<FloatT>(dbl_v, n, 5, f, mask);

    else if constexpr (std::is_same_v<T, CharT>) {
        unsigned int i2 = 0;
        while (i2 < matr_idx[1].size() && n != matr_idx[1][i2])
            ++i2;
        const unsigned int start = nrow * i2;
        unsigned int i3 = 0;
        std::vector<std::string>& val_tmp = tmp_val_refv[n];
        const unsigned int end_val = mask.size();
        for (size_t i = start; i < start + end_val; ++i, ++i3) {
            if (!mask[i3]) {
              continue;
            }
            f(chr_v[i]);
            val_tmp[i3].assign(chr_v[i], df_charbuf_size);
        }
    }

    else if constexpr (std::is_same_v<T, std::string>) {
        unsigned int i2 = 0;
        while (i2 < matr_idx[0].size() && n != matr_idx[0][i2])
            ++i2;
        const unsigned int start = nrow * i2;
        unsigned int i3 = 0;
        std::vector<std::string>& val_tmp = tmp_val_refv[n];
        const unsigned int end_val = mask.size();
        for (size_t i = start; i < start + end_val; ++i, ++i3) {
            if (!mask[i3]) {
              continue;
            }
            f(str_v[i]);
            val_tmp[i3] = str_v[i];
        }
    }
}



