#pragma once

template <typename F, bool IsBool = false>
requires FapplyFn<F, first_arg_t<F>>
void fapply_filter_idx(F f, 
                unsigned int& n, 
                const std::vector<unsigned int>& mask) 
{

    using T = first_arg_t<F>;

    if constexpr (IsBool) {

        if constexpr (!(std::is_same_v<T, uint8_t>)) {
          std::cerr << "A bool must be uint8_t\n";
          return;
        }

        apply_numeric_filter_idx<uint8_t>(bool_v, n, 0, f, mask);

    } else if constexpr (std::is_same_v<T, IntT>)
        apply_numeric_filter_idx<IntT>(int_v, n, 3, f, mask);

    else if constexpr (std::is_same_v<T, UIntT>)
        apply_numeric_filter_idx<UIntT>(uint_v, n, 4, f, mask);

    else if constexpr (std::is_same_v<T, FloatT>)
        apply_numeric_filter_idx<FloatT>(dbl_v, n, 5, f, mask);

    else if constexpr (std::is_same_v<T, char>) {
        unsigned int i2 = 0;
        while (i2 < matr_idx[1].size() && n != matr_idx[1][i2])
            ++i2;
        
        const unsigned int start = nrow * i2;
        std::vector<std::string>& val_tmp = tmp_val_refv[n];
        
        #if defined(__clang__)
          #pragma clang loop vectorize(enable)
        #elif defined(__GNUC__)
            #pragma GCC ivdep
        #elif defined(_MSC_VER)
            #pragma loop(ivdep)
        #endif
        
        for (unsigned int& pos_idx : mask) {
            const unsigned int abs_idx = start + pos_idx;
            f(chr_v[abs_idx]);
            val_tmp[pos_idx].assign(chr_v[abs_idx], df_charbuf_size);
        }

    }

    else if constexpr (std::is_same_v<T, std::string>) {
        unsigned int i2 = 0;
        while (i2 < matr_idx[0].size() && n != matr_idx[0][i2])
            ++i2;
        
        const unsigned int start = nrow * i2;
        std::vector<std::string>& val_tmp = tmp_val_refv[n];

        #if defined(__clang__)
          #pragma clang loop vectorize(enable)
        #elif defined(__GNUC__)
            #pragma GCC ivdep
        #elif defined(_MSC_VER)
            #pragma loop(ivdep)
        #endif
        for (unsigned int& pos_idx : mask) {
            const unsigned int abs_idx = start + pos_idx;
            f(str_v[abs_idx]);
            val_tmp[pos_idx] = str_v[abs_idx];
        }

    }
}



