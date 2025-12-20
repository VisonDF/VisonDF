#pragma once

std::vector<const IntT>& view_colint(unsigned int x) const {
    const auto& pos_idx = matr_idx[3];
    for (size_t i2 = 0; i2 < pos_idx.size(); ++i2) {
        if (x == pos_idx[i2]) {
            return int_v[i2];
        };
    };
};


