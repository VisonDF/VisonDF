#pragma once

template <bool MapCol = false,
          typename T, 
          typename F>
inline void apply_numeric_filter_boolmask(const std::vector<T>& values, 
                                          const unsigned int n, 
                                          const size_t idx_type, 
                                          F&& f,
                                          const std::vector<uint8_t>& mask,
                                          const unsigned int strt_vl) 
{
    unsigned int i2 = 0;
    if constexpr (!MapCol) {
        i2 = 0;
        while (i2 < matr_idx[idx_type].size()) {
            if (n == matr_idx[idx_type][i2])
                break;
            ++i2;
        }
    } else {
        if (!matr_idx_map[idx_type].contains(n)) {
            std::cerr << "MapCol used but col not found in the map\n";
            return;
        }
        if (!sync_map_col[idx_type]) {
            std::cerr << "Col not synced\n";
            return;
        }

        i2 = matr_idx_map[idx_type][n];

    }

    std::vector<T>& dst = values[i2];
    const unsigned int end_val = mask.size();

    for (size_t i = 0; i < end_val; ++i) {
        if (!mask[i]) [[likely]] { continue; }
        f(dst[strt_vl + i]);
    }
}


