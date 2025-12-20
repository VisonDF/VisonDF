#pragma once

using ColumnView = std::variant<
    std::span<const std::string>&,
    std::span<const CharT>&,
    std::span<const uint8_t>&,
    std::span<const IntT>&,
    std::span<const UIntT>&,
    std::span<const FloatT>&
>;

ColumnView view_col(unsigned int x) const {

    for (unsigned int i = 0; i < 6; ++i) {
        for (unsigned int i2 = 0; i2 < matr_idx[i].size(); ++i2) {
            if (x == matr_idx[i][i2]) {
                switch (i) {
                    case 0:
                        return str_v[i2];
                    case 1:
                        return chr_v[i2];
                    case 2:
                        return bool_v[i2];
                    case 3:
                        return int_v[i2];
                    case 4:
                        return uint_v[i2];
                    case 5:
                        return dbl_v[i2];
                }
            }
        }
    }

    throw std::out_of_range("view_colnb(): column not found");
}


