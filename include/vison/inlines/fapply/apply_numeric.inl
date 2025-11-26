#pragma once

template <typename T, typename F>
inline void apply_numeric(std::vector<T>& values, unsigned int n, size_t idx_type, F&& f) {
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

    for (size_t i = start; i < start + nrow; ++i, ++i3) {
        f(values[i]);

        char buf[buf_size];
        auto [ptr, ec] = fast_to_chars(buf, buf + buf_size, values[i]);

        if (ec == std::errc{}) [[likely]] {
            val_tmp[i3].assign(buf, ptr);
        } else [[unlikely]] {
            std::terminate();
        }
    }
}


