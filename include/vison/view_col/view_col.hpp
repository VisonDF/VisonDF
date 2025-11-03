#pragma once

using ColumnView = std::variant<
    std::span<const std::string>,
    std::span<const char>,
    std::span<const int>,
    std::span<const unsigned int>,
    std::span<const double>
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
                        return std::span<const char>(chr_v.data() + offset, nrow);
                    case 2:
                        std::cerr << "Can't view boolean column\n";
                        break;
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


