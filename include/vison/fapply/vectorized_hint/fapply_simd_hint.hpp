#pragma once

template <typename VecT, typename T, typename F>
inline void apply_numeric_simd(VecT& values, 
                unsigned int n, 
                size_t idx_type, 
                F&& f) {
    
    constexpr size_t buf_size = max_chars_needed<T>();
    
    for (auto& s : tmp_val_refv[n])
        s.reserve(buf_size);

    unsigned int i2 = 0;
    while (i2 < matr_idx[idx_type].size() && n != matr_idx[idx_type][i2])
        ++i2;

    const unsigned int start = nrow * i2;
    const unsigned int end   = start + nrow;

    unsigned int i3 = 0;
    size_t i = 0;
    
#if defined(__clang__)
    #pragma clang loop vectorize(enable)
#elif defined(__GNUC__)
    #pragma GCC ivdep
#elif defined(_MSC_VER)
    #pragma loop(ivdep)
#endif

    for (; i + 4 <= end; i += 4, i3 += 4) {
        f(values[i + 0]);
        f(values[i + 1]);
        f(values[i + 2]);
        f(values[i + 3]);

        char buf[buf_size];
        for (int j = 0; j < 4; ++j) {
            auto [ptr, ec] = std::to_chars(buf, buf + buf_size, values[i + j]);
            if (ec == std::errc{}) [[likely]]
                tmp_val_refv[n][i3 + j].assign(buf, ptr);
            else [[unlikely]]
                std::terminate();
        }
    }

    for (; i < end; ++i, ++i3) {
        f(values[i]);
        char buf[buf_size];
        auto [ptr, ec] = std::to_chars(buf, buf + buf_size, values[i]);
        if (ec == std::errc{}) [[likely]]
            tmp_val_refv[n][i3].assign(buf, ptr);
        else [[unlikely]]
            std::terminate();
    }
}


template <typename T>
void fapply_simd(void (&f)(T&), unsigned int& n) {

    if constexpr (std::is_same_v<T, bool>)
        apply_numeric_simd<decltype(bool_v), bool>(bool_v, n, 0, f);

    else if constexpr (std::is_same_v<T, IntT>)
        apply_numeric_simd<decltype(int_v), IntT>(int_v, n, 3, f);

    else if constexpr (std::is_same_v<T, UIntT>)
        apply_numeric_simd<decltype(uint_v), UIntT>(uint_v, n, 4, f);

    else if constexpr (std::is_same_v<T, FloatT>)
        apply_numeric_simd<decltype(dbl_v), FloatT>(dbl_v, n, 5, f);

    else if constexpr (std::is_same_v<T, char>) {
        unsigned int i2 = 0;
        while (i2 < matr_idx[1].size() && n != matr_idx[1][i2])
            ++i2;
        const unsigned int start = nrow * i2;
        unsigned int i3 = 0;
        for (size_t i = start; i < start + nrow; ++i, ++i3) {
            f(chr_v[i]);
            tmp_val_refv[n][i3].assign(1, chr_v[i]);
            i3 += 1;
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
            i3 += 1;
        }
    }
}


