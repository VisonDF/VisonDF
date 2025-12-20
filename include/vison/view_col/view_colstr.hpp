#pragma once

std::vector<const std::string>& view_colstr(unsigned int x) const {
    const auto& pos_idx = matr_idx[0];
    for (size_t i2 = 0; i2 < pos_idx.size(); ++i2) {
        if (x == pos_idx[i2]) {
            return str_v[i2];
        };
    };
};


