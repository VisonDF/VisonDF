#pragma once

template <typename VecT, typename T, typename F>
inline void apply_numeric_filter_range(VecT& values, 
                unsigned int n, 
                size_t idx_type, 
                F&& f,
                const std::vector<uint8_t>& mask,
                const unsigned int& strt_vl) 
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
    const unsigned int end_val = mask.size();

    for (size_t i = start; i < start + end_val; ++i, ++i3) {

        if (!mask[i3]) [[likely]] {
          continue;
        }

        f(values[strt_vl + i]);

        char buf[buf_size];
        auto [ptr, ec] = std::to_chars(buf, buf + buf_size, values[strt_vl + i]);

        if (ec == std::errc{}) [[likely]] {
            val_tmp[i3].assign(buf, ptr);
        } else [[unlikely]] {
            std::terminate();
        }
    }
}


