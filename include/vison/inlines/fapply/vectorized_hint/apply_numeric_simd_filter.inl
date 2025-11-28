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
    std::vector<T>& dst = values[i2];

    size_t i = 0;

    std::vector<std::string>& val_tmp = tmp_val_refv[n];
     
    #if defined(__clang__)
        #pragma clang loop vectorize(enable)
    #elif defined(__GNUC__)
        #pragma GCC ivdep
    #elif defined(_MSC_VER)
        #pragma loop(ivdep)
    #endif
        
    for (; i + 4 <= end_val; i += 4) {
    
        char buf[buf_size];

        auto process = [&](int k) {
            if (!mask[i + k]) return;

            f(dst[i + k]);

            auto [ptr, ec] = fast_to_chars(buf, buf + buf_size, dst[i + k]);
            if (ec == std::errc{}) [[likely]] {
                val_tmp[i + k].assign(buf, ptr);
            } else [[unlikely]] {
                std::terminate();
            }
        };

        process(0);
        process(1);
        process(2);
        process(3);

    }

    for (; i < end_val; ++i) {
        if (mask[i]) {
            f(dst[i]);
            char buf[buf_size];
            auto [ptr, ec] = fast_to_chars(buf, buf + buf_size, dst[i]);
            if (ec == std::errc{}) [[likely]]
                val_tmp[i].assign(buf, ptr);
            else [[unlikely]]
                std::terminate();
        }
    }
}


