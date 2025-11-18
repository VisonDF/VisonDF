#pragma once

template <typename F>
requires FapplyFn<F, first_arg_t<F>>
void fapply_simd_filter_idx(F f, 
                unsigned int& n,
                const std::vector<unsigned int>& mask) {

    using T = first_arg_t<F>;

    if constexpr (std::is_same_v<T, bool>)
        apply_numeric_simd_filter_idx<decltype(bool_v), bool>(bool_v, n, 0, f);

    else if constexpr (std::is_same_v<T, IntT>)
        apply_numeric_simd_filter_idx<decltype(int_v), IntT>(int_v, n, 3, f);

    else if constexpr (std::is_same_v<T, UIntT>)
        apply_numeric_simd_filter_idx<decltype(uint_v), UIntT>(uint_v, n, 4, f);

    else if constexpr (std::is_same_v<T, FloatT>)
        apply_numeric_simd_filter_idx<decltype(dbl_v), FloatT>(dbl_v, n, 5, f);

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
            val_tmp[pos_idx].assign(1, chr_v[abs_idx]);
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
        
        for (auto& pos_idx : mask) {
            const unsigned int abs_idx = start + pos_idx;
            f(str_v[abs_idx]);
            val_tmp[pos_idx] = str_v[abs_idx];
        }

    }
}


