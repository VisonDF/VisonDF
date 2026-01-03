#pragma once

template <int Container = -1>
void mapcol()  
{
    if constexpr (Container == -1) {
        for (size_t i = 0; i < 6; ++i) {
            const auto& mp = matr_idx_map[i];
            mp.clear();
            const auto& cur_matr_idx = matr_idx[i];
            for (size_t i2 = 0; i2 < cur_matr_idx.size(); ++i2)
                mp.emplace(cur_matr_idx[i2], i2);
            i += 1;
        }
    } else {
        const auto& mp = matr_idx_map[Container];
        mp.clear();
        const auto& cur_matr_idx = matr_idx[Container];
        for (size_t i2 = 0; i2 < cur_matr_idx.size(); ++i2)
            mp.emplace(cur_matr_idx[i2], i2);
    }
}

