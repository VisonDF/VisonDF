#pragma once

template <typename VecT, typename T, typename F>
inline void apply_numeric_filter_idx(VecT& values, 
                unsigned int n, 
                size_t idx_type, 
                F&& f,
                const std::vector<unsigned>& mask) 
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
    unsigned int i3 = 0;
    std::vector<std::string>& val_tmp = tmp_val_refv[n];

    for (; i3 < mask.size(); ++i3) {

        unsigned int pos_idx = mask[i3];
        unsigned int abs_idx = start + mask[i3];

        f(values[abs_idx]);

        char buf[buf_size];
        auto [ptr, ec] = std::to_chars(buf, buf + buf_size, values[abs_idx]);

        if (ec == std::errc{}) [[likely]] {
            val_tmp[pos_idx].assign(buf, ptr);
        } else [[unlikely]] {
            std::terminate();
        }
    }
}
