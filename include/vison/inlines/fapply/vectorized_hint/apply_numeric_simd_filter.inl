#pragma once

template <typename VecT, typename T, typename F>
inline void apply_numeric_simd_filter(VecT& values, 
                unsigned int n, 
                size_t idx_type, 
                F&& f,
                const std::vector<uint8_t>& mask) {
    
    constexpr size_t buf_size = max_chars_needed<T>();
    
    for (auto& s : tmp_val_refv[n])
        s.reserve(buf_size);

    unsigned int i2 = 0;
    while (i2 < matr_idx[idx_type].size() && n != matr_idx[idx_type][i2])
        ++i2;

    const unsigned int end_val = mask.size();
    const unsigned int start = nrow * i2;
    const unsigned int end   = start + end_val;

    unsigned int i3 = 0;
    size_t i = 0;

    std::vector<std::string>& val_tmp = tmp_val_refv[n];
    
    char buf[buf_size];

#if defined(__clang__)
    #pragma clang loop vectorize(enable)
#elif defined(__GNUC__)
    #pragma GCC ivdep
#elif defined(_MSC_VER)
    #pragma loop(ivdep)
#endif
        
    for (; i + 4 <= end; i += 4, i3 += 4) {
        
        if (mask[i]) {

            f(values[i + 0]);
       
            auto [ptr, ec] = std::to_chars(buf, buf + buf_size, values[i]);
            if (ec == std::errc{}) [[likely]]
                val_tmp[i3].assign(buf, ptr);
            else [[unlikely]]
                std::terminate();

        }

        if (mask[i + 1]) {

            f(values[i + 1]);

            auto [ptr, ec] = std::to_chars(buf, buf + buf_size, values[i + 1]);
            if (ec == std::errc{}) [[likely]]
                val_tmp[i3 + 1].assign(buf, ptr);
            else [[unlikely]]
                std::terminate();

        }

        if (mask[i + 2]) {

            f(values[i + 2]);

            auto [ptr, ec] = std::to_chars(buf, buf + buf_size, values[i + 2]);
            if (ec == std::errc{}) [[likely]]
                val_tmp[i3 + 2].assign(buf, ptr);
            else [[unlikely]]
                std::terminate();

        }
        if (mask[i + 3]) {

            f(values[i + 3]);

            auto [ptr, ec] = std::to_chars(buf, buf + buf_size, values[i + 3]);
            if (ec == std::errc{}) [[likely]]
                val_tmp[i3 + 3].assign(buf, ptr);
            else [[unlikely]]
                std::terminate();

        }
                
    }

    for (; i < end; ++i, ++i3) {
        if (mask[i]) {
            f(values[i]);
            char buf[buf_size];
            auto [ptr, ec] = std::to_chars(buf, buf + buf_size, values[i]);
            if (ec == std::errc{}) [[likely]]
                val_tmp[i3].assign(buf, ptr);
            else [[unlikely]]
                std::terminate();
        }
    }
}


