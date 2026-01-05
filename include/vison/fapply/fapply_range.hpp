#pragma once

template <bool IsBool = false,
          bool MapCol = false,
          typename F>
requires FapplyFn<F, first_arg_t<F>>
void fapply_range(F f, 
                  const unsigned int n,
                  const unsigned int strt,
                  const unsigned int end) {

    using T = first_arg_t<F>;
    const unsigned int local_nrow = nrow;

    if constexpr (IsBool) {

        if constexpr (!(std::is_same_v<T, uint8_t>)) {
          std::cerr << "A bool must be uint8_t\n";
          return;
        }

        apply_numeric<MapCol>(bool_v, 
                              n, 
                              2, 
                              f, 
                              strt, 
                              end);

    } else if constexpr (std::is_same_v<T, IntT>) {

        apply_numeric<MapCol>(int_v, 
                              n, 
                              3, 
                              f, 
                              strt, 
                              end);

    } else if constexpr (std::is_same_v<T, UIntT>) {

        apply_numeric<MapCol>(uint_v, 
                              n, 
                              4, 
                              f, 
                              strt, 
                              end);

    } else if constexpr (std::is_same_v<T, FloatT>) {

        apply_numeric<MapCol>(dbl_v, 
                              n, 
                              5, 
                              f, 
                              strt, 
                              end);

    } else if constexpr (std::is_same_v<T, CharT>) {
        unsigned int i2;
        if constexpr (!MapCol) {
            i2 = 0;
            while (i2 < matr_idx[1].size() && n != matr_idx[1][i2])
                ++i2;
        } else {

            if (!matr_idx_map[1].contains(n)) {
                std::cerr << "MapCol used but col not found in the map\n";
                return;
            }
            if (!sync_map_col[1]) {
                std::cerr << "Col not synced\n";
                return;
            }

            i2 = matr_idx_map[1][n];
        }

        std::vector<CharT>& dst = chr_v[i2];

        for (size_t i = strt; i < end; ++i)
            f(dst[i]);
        
    } else if constexpr (std::is_same_v<T, std::string>) {
        unsigned int i2;
        if constexpr (!MapCol) {
            i2 = 0;
            while (i2 < matr_idx[0].size() && n != matr_idx[0][i2])
                ++i2;
        } else {
            if (!matr_idx_map[0].contains(n)) {
                std::cerr << "MapCol used but col not found in the map\n";
                return;
            }
            if (!sync_map_col[0]) {
                std::cerr << "Col not synced\n";
                return;
            }

            i2 = matr_idx_map[0][n];
        }

        std::vector<std::string>& dst = str_v[i2];

        for (size_t i = strt; i < end; ++i)
            f(dst[i]);
    }
}



