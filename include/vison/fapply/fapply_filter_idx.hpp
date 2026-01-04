#pragma once

template <bool IsBool = false,
          bool MapCol = false,
          typename F>
requires FapplyFn<F, first_arg_t<F>>
void fapply_filter_idx(F f, 
                       const unsigned int n, 
                       const std::vector<unsigned int>& mask) 
{

    using T = first_arg_t<F>;

    if constexpr (IsBool) {

        if constexpr (!(std::is_same_v<T, uint8_t>)) {
          std::cerr << "A bool must be uint8_t\n";
          return;
        }

        apply_numeric_filter_idx<MapCol>(bool_v, 
                                          n, 
                                          0, 
                                          f, 
                                          mask);

    } else if constexpr (std::is_same_v<T, IntT>) {

        apply_numeric_filter_idx<MapCol>(int_v, 
                                       n, 
                                       3, 
                                       f, 
                                       mask);

    } else if constexpr (std::is_same_v<T, UIntT>) {

        apply_numeric_filter_idx<MapCol>(uint_v, 
                                        n, 
                                        4, 
                                        f, 
                                        mask);

    } else if constexpr (std::is_same_v<T, FloatT>) {

        apply_numeric_filter_idx<MapCol>(dbl_v, 
                                         n, 
                                         5, 
                                         f, 
                                         mask);

    } else if constexpr (std::is_same_v<T, CharT>) {
        unsigned int i2;
        i2 = 0;
        if constexpr (!MapCol) {
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
        
        #if defined(__clang__)
          #pragma clang loop vectorize(enable)
        #elif defined(__GNUC__)
            #pragma GCC ivdep
        #elif defined(_MSC_VER)
            #pragma loop(ivdep)
        #endif
        
        for (unsigned int& pos_idx : mask) 
            f(dst[pos_idx]);

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

        #if defined(__clang__)
          #pragma clang loop vectorize(enable)
        #elif defined(__GNUC__)
            #pragma GCC ivdep
        #elif defined(_MSC_VER)
            #pragma loop(ivdep)
        #endif
        for (unsigned int& pos_idx : mask)
            f(dst[pos_idx]);

    }
}



