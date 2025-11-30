#pragma once

template <typename F, bool IsBool = false>
requires FapplyFn<F, first_arg_t<F>>
void fapply_simd(F f, unsigned int& n) {

    using T = first_arg_t<F>;
    const unsigned int local_nrow = nrow;

    if constexpr (IsBool) {
        
        if constexpr (!(std::is_same_v<T, uint8_t>)) {
          std::cerr << "A bool must be uint8_t\n";
          return;
        }
        
        apply_numeric_simd<uint8_t>(bool_v, 
                                    n, 
                                    0, 
                                    f,
                                    local_nrow);

    } else if constexpr (std::is_same_v<T, IntT>)
        apply_numeric_simd<IntT>(int_v, 
                                 n, 
                                 3, 
                                 f,
                                 local_nrow);

    else if constexpr (std::is_same_v<T, UIntT>)
        apply_numeric_simd<UIntT>(uint_v, 
                                  n, 
                                  4, 
                                  f,
                                  local_nrow);

    else if constexpr (std::is_same_v<T, FloatT>)
        apply_numeric_simd<FloatT>(dbl_v, 
                                   n, 
                                   5, 
                                   f,
                                   local_nrow);

    else if constexpr (std::is_same_v<T, CharT>) {
        unsigned int i2 = 0;
        while (i2 < matr_idx[1].size() && n != matr_idx[1][i2])
            ++i2;
        std::vector<CharT>& dst = chr_v[i2];
        std::vector<std::string>& val_tmp = tmp_val_refv[n];
        for (size_t i = 0; i < local_nrow; ++i) {
            f(dst[i]);
            val_tmp[i].assign(dst[i], df_charbuf_size);
        }
    }

    else if constexpr (std::is_same_v<T, std::string>) {
        unsigned int i2 = 0;
        while (i2 < matr_idx[0].size() && n != matr_idx[0][i2])
            ++i2;
        std::vector<std::string>& dst = str_v[i2];
        std::vector<std::string>& val_tmp = tmp_val_refv[n];
        for (size_t i = 0; i < start + local_nrow; ++i) {
            f(dst[i]);
            val_tmp[i] = dst[i];
        }
    }
}


