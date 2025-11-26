#pragma once

template <typename T, typename F>
inline void apply_numeric_simd_filter(std::vector<T>& values, 
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
    size_t i = start;

    std::vector<std::string>& val_tmp = tmp_val_refv[n];
     
    #if defined(__clang__)
        #pragma clang loop vectorize(enable)
    #elif defined(__GNUC__)
        #pragma GCC ivdep
    #elif defined(_MSC_VER)
        #pragma loop(ivdep)
    #endif
        
    for (; i + 4 <= end; i += 4, i3 += 4) {
    
        char buf[buf_size];

        auto process = [&](int k) {
            if (!mask[i + k]) return;

            f(values[i + k]);

            auto [ptr, ec] = fast_to_chars(buf, buf + buf_size, values[i + k]);
            if (ec == std::errc{}) [[likely]] {
                val_tmp[i3 + k].assign(buf, ptr);
            } else [[unlikely]] {
                std::terminate();
            }
        };

        process(0);
        process(1);
        process(2);
        process(3);

    }

    for (; i < end; ++i, ++i3) {
        if (mask[i]) {
            f(values[i]);
            char buf[buf_size];
            auto [ptr, ec] = fast_to_chars(buf, buf + buf_size, values[i]);
            if (ec == std::errc{}) [[likely]]
                val_tmp[i3].assign(buf, ptr);
            else [[unlikely]]
                std::terminate();
        }
    }
}


