#pragma once

using ColumnView = std::variant<
    std::span<const int>,
    std::span<const unsigned int>,
    std::span<const double>
>;

ColumnView view_colnb(unsigned int &x) const {
    unsigned int i2 = 0;

    for (unsigned int i = 3; i <= 5; ++i) {
        for (i2 = 0; i2 < matr_idx[i].size(); ++i2) {
            if (x == matr_idx[i][i2]) {
                const size_t offset = static_cast<size_t>(nrow) * i2;

                switch (i) {
                    case 3:
                        return std::span<const int>(int_v.data() + offset, nrow);
                    case 4:
                        return std::span<const unsigned int>(uint_v.data() + offset, nrow);
                    case 5:
                        return std::span<const double>(dbl_v.data() + offset, nrow);
                }
            }
        }
    }

    throw std::out_of_range("view_colnb(): column not found");
}


