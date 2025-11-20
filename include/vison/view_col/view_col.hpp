#pragma once

using ColumnView = std::variant<
    std::span<const std::string>,
    std::span<const CharT>,
    std::span<const uint8_t>,
    std::span<const IntT>,
    std::span<const UIntT>,
    std::span<const FloatT>
>;

ColumnView view_col(unsigned int &x) const {
    unsigned int i2 = 0;

    for (unsigned int i = 0; i < 6; ++i) {
        for (i2 = 0; i2 < matr_idx[i].size(); ++i2) {
            if (x == matr_idx[i][i2]) {
                const size_t offset = static_cast<size_t>(nrow) * i2;

                switch (i) {
                    case 0:
                        return std::span<const std::string>(str_v.data() + offset, nrow);
                    case 1:
                        return std::span<const CharT>(chr_v.data() + offset, nrow);
                    case 2:
                        return std::span<const uint8_t>(bool_v.data() + offset, nrow);
                    case 3:
                        return std::span<const IntT>(int_v.data() + offset, nrow);
                    case 4:
                        return std::span<const UIntT>(uint_v.data() + offset, nrow);
                    case 5:
                        return std::span<const FloatT>(dbl_v.data() + offset, nrow);
                }
            }
        }
    }

    throw std::out_of_range("view_colnb(): column not found");
}


