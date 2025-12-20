#pragma once

std::vector<const uint8_t>& view_colbool(unsigned int x) const {
    const auto& pos_idx = matr_idx[2];
    for (size_t i2 = 0; i2 < pos_idx.size(); ++i2) {
        if (x == pos_idx[i2]) {
            return bool_v[i2];
        };
    };
};


