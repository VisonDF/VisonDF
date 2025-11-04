#pragma once

template <typename VecT, typename T, typename F>
inline void apply_numeric(VecT& values, 
                unsigned int n, 
                size_t idx_type, 
                F&& f,
                const std::vector<uint8_t>& mask) 
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

    for (size_t i = start; i < start + nrow; ++i, ++i3) {

        if (!mask[i3]) [[likely]] {
          continue;
        }

        f(values[i]);

        char buf[buf_size];
        auto [ptr, ec] = std::to_chars(buf, buf + buf_size, values[i]);

        if (ec == std::errc{}) [[likely]] {
            val_tmp[i3].assign(buf, ptr);
        } else [[unlikely]] {
            std::terminate();
        }
    }
}

template <typename T>
void fapply(void (&f)(T&), 
                unsigned int& n, 
                const std::vector<uint8_t>& mask) 
{

    if constexpr (std::is_same_v<T, bool>)
        apply_numeric<decltype(bool_v), bool>(bool_v, n, 0, f, mask);

    else if constexpr (std::is_same_v<T, IntT>)
        apply_numeric<decltype(int_v), IntT>(int_v, n, 3, f, mask);

    else if constexpr (std::is_same_v<T, UIntT>)
        apply_numeric<decltype(uint_v), UIntT>(uint_v, n, 4, f, mask);

    else if constexpr (std::is_same_v<T, FloatT>)
        apply_numeric<decltype(dbl_v), FloatT>(dbl_v, n, 5, f, mask);

    else if constexpr (std::is_same_v<T, char>) {
        unsigned int i2 = 0;
        while (i2 < matr_idx[1].size() && n != matr_idx[1][i2])
            ++i2;
        const unsigned int start = nrow * i2;
        unsigned int i3 = 0;
        std::vector<std::string>& val_tmp = tmp_val_refv[n];
        for (size_t i = start; i < start + nrow; ++i, ++i3) {
            f(chr_v[i]);
            val_tmp[i3].assign(1, chr_v[i]);
        }
    }

    else if constexpr (std::is_same_v<T, std::string>) {
        unsigned int i2 = 0;
        while (i2 < matr_idx[0].size() && n != matr_idx[0][i2])
            ++i2;
        const unsigned int start = nrow * i2;
        unsigned int i3 = 0;
        std::vector<std::string>& val_tmp = tmp_val_refv[n];
        for (size_t i = start; i < start + nrow; ++i, ++i3) {
            f(str_v[i]);
            val_tmp[i3] = str_v[i];
        }
    }
}



