#pragma once

template <typename T, typename F>
inline void apply_numeric_simd_filter_idx(
    std::vector<T>& values,
    unsigned int n,
    size_t idx_type,
    F&& f,
    const std::vector<unsigned int>& mask) noexcept
{
    constexpr size_t buf_size = max_chars_needed<T>();

    for (auto& s : tmp_val_refv[n])
        s.reserve(buf_size);

    unsigned int i2 = 0;
    while (i2 < matr_idx[idx_type].size() && n != matr_idx[idx_type][i2])
        ++i2;

    const unsigned int start = nrow * i2;
    auto& val_tmp = tmp_val_refv[n];
    size_t i3 = 0;

#if defined(__clang__)
    #pragma clang loop vectorize(enable)
#elif defined(__GNUC__)
    #pragma GCC ivdep
#elif defined(_MSC_VER)
    #pragma loop(ivdep)
#endif
    for (; i3 + 4 <= mask.size(); i3 += 4) {
        const unsigned int idx0 = mask[i3 + 0];
        const unsigned int idx1 = mask[i3 + 1];
        const unsigned int idx2 = mask[i3 + 2];
        const unsigned int idx3 = mask[i3 + 3];
    
        const unsigned int abs0 = start + idx0;
        const unsigned int abs1 = start + idx1;
        const unsigned int abs2 = start + idx2;
        const unsigned int abs3 = start + idx3;
    
        f(values[abs0]);
        f(values[abs1]);
        f(values[abs2]);
        f(values[abs3]);
    
        char buf[buf_size];
        auto store = [&](unsigned int pos, unsigned int abs) {
            auto [ptr, ec] = fast_to_chars(buf, buf + buf_size, values[abs]);
            if (ec == std::errc{}) [[likely]]
                val_tmp[pos].assign(buf, ptr);
            else [[unlikely]]
                std::terminate();
        };
    
        store(idx0, abs0);
        store(idx1, abs1);
        store(idx2, abs2);
        store(idx3, abs3);
    }

    for (; i3 < mask.size(); ++i3) {
        const unsigned int pos_idx = mask[i3];
        const unsigned int abs_idx = start + pos_idx;
    
        f(values[abs_idx]);
        char buf[buf_size];
        auto [ptr, ec] = fast_to_chars(buf, buf + buf_size, values[abs_idx]);
        if (ec == std::errc{}) [[likely]]
            val_tmp[pos_idx].assign(buf, ptr);
        else [[unlikely]]
            std::terminate();
    }

}



