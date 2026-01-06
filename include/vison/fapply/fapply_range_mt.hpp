#pragma once

template <bool IsBool = false,
          bool MapCol = false,
          unsigned int CORES = 4,
          bool NUMA = false,
          typename F>
requires FapplyFn<F, first_arg_t<F>>
void fapply_range_mt(F f, 
                     const unsigned int n,
                     const unsigned int strt,
                     const unsigned int end) {

    using T = first_arg_t<F>;

    if constexpr (IsBool) {

        if constexpr (!(std::is_same_v<T, uint8_t>)) {
          std::cerr << "A bool must be uint8_t\n";
          return;
        }

        apply_numeric<MapCol, CORES, NUMA>(bool_v, 
                                           n, 
                                           2, // idx_type
                                           f, 
                                           strt, 
                                           end);

    } else if constexpr (std::is_same_v<T, IntT>) {

        apply_numeric<MapCol, CORES, NUMA>(int_v, 
                                           n, 
                                           3, // idx_type
                                           f, 
                                           strt, 
                                           end);

    } else if constexpr (std::is_same_v<T, UIntT>) {

        apply_numeric<MapCol, CORES, NUMA>(uint_v, 
                                           n, 
                                           4, // idx_type
                                           f, 
                                           strt, 
                                           end);

    } else if constexpr (std::is_same_v<T, FloatT>) {

        apply_numeric<MapCol, CORES, NUMA>(dbl_v, 
                                           n, 
                                           5, // idx_type
                                           f, 
                                           strt, 
                                           end);

    } else if constexpr (std::is_same_v<T, CharT>) {

        apply_numeric<MapCol, CORES, NUMA>(dbl_v, 
                                           n, 
                                           1, // idx_type
                                           f, 
                                           strt, 
                                           end);

        
    } else if constexpr (std::is_same_v<T, std::string>) {

        apply_numeric<MapCol, CORES, NUMA>(dbl_v, 
                                           n, 
                                           0, // idx_type
                                           f, 
                                           strt, 
                                           end);

    }
}



