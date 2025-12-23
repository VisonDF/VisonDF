#pragma once

void mapcol()  
{
    for (size_t i = 0; i < 6; ++i) {
        const auto& mp = matr_idx_map[i];
        mp.clear();
        const auto& cur_matr_idx = matr_idx[i];
        for (size_t i2 = 0; i2 < cur_matr_idx.size(); ++i2)
            mp.emplace(cur_matr_idx[i2], i2);
        i += 1;
    }
}

