#pragma once

template <typename T, typename F>
inline void apply_numeric_filter_idx(std::vector<T>& values, 
                unsigned int n, 
                size_t idx_type, 
                F&& f,
                const std::vector<unsigned int>& mask) 
{
    constexpr auto buf_size = max_chars_needed<T>();
    for (auto& s : tmp_val_refv[n])
        s.reserve(buf_size);

    unsigned int i2 = 0;
    while (i2 < matr_idx[idx_type].size()) {
        if (n == matr_idx[idx_type][i2])
            break;
        ++i2;
    }

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

        char buf[buf_size];

        const unsigned int abs_idx = start + pos_idx;

        f(values[abs_idx]);

        auto [ptr, ec] = fast_to_chars(buf, buf + buf_size, values[abs_idx]);

        if (ec == std::errc{}) [[likely]] {
            val_tmp[pos_idx].assign(buf, ptr);
        } else [[unlikely]] {
            std::terminate();
        }
    }

}


