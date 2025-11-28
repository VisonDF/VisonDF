#pragma once

template <typename T, typename F>
inline void apply_numeric_simd(std::vector<T>& values, 
                unsigned int n, 
                size_t idx_type, 
                F&& f) {
    
    constexpr size_t buf_size = max_chars_needed<T>();
    
    for (auto& s : tmp_val_refv[n])
        s.reserve(buf_size);

    unsigned int i2 = 0;
    while (i2 < matr_idx[idx_type].size() && n != matr_idx[idx_type][i2])
        ++i2;

    std::vector<T>& dst    = values[i2];
    size_t i = 0;

    std::vector<std::string>& val_tmp = tmp_val_refv[n];

    
    #if defined(__clang__)
        #pragma clang loop vectorize(enable)
    #elif defined(__GNUC__)
        #pragma GCC ivdep
    #elif defined(_MSC_VER)
        #pragma loop(ivdep)
    #endif
        
    for (; i + 4 <= nrow; i += 4) {
        f(dst[i + 0]);
        f(dst[i + 1]);
        f(dst[i + 2]);
        f(dst[i + 3]);

        char buf[buf_size];
        for (int j = 0; j < 4; ++j) {
            auto [ptr, ec] = fast_to_chars(buf, buf + buf_size, dst[i + j]);
            if (ec == std::errc{}) [[likely]]
                val_tmp[i + j].assign(buf, ptr);
            else [[unlikely]]
                std::terminate();
        }
    }

    for (; i < nrow; ++i) {
        f(dst[i]);
        char buf[buf_size];
        auto [ptr, ec] = fast_to_chars(buf, buf + buf_size, dst[i]);
        if (ec == std::errc{}) [[likely]]
            val_tmp[i].assign(buf, ptr);
        else [[unlikely]]
            std::terminate();
    }
}


