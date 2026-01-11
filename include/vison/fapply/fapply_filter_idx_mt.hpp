#pragma once

template <bool IsBool = false,
          bool MapCol = false,
          unsigned int CORES = 4,
          bool NUMA = false,
          bool IdxIsTrue = true,
          typename F>
requires FapplyFn<F, first_arg_t<F>>
void fapply_filter_idx_mt(F f, 
                          const unsigned int n, 
                          const std::vector<unsigned int>& mask) 
{

    using T = first_arg_t<F>;

    if constexpr (IsBool) {

        if constexpr (!(std::is_same_v<T, uint8_t>)) {
          std::cerr << "A bool must be uint8_t\n";
          return;
        }

        apply_numeric_filter_idx<MapCol, 
                                 CORES, 
                                 NUMA,
                                 IdxIsTrue>(bool_v, 
                                            n, 
                                            2, // idx_type
                                            f, 
                                            mask);

    } else if constexpr (std::is_same_v<T, IntT>) {

        apply_numeric_filter_idx<MapCol, 
                                 CORES, 
                                 NUMA, 
                                 IdxIsTrue>(int_v, 
                                            n, 
                                            3,  // idx_type 
                                            f, 
                                            mask);

    } else if constexpr (std::is_same_v<T, UIntT>) {

        apply_numeric_filter_idx<MapCol, 
                                 CORES, 
                                 NUMA, 
                                 IdxIsTrue>(uint_v, 
                                            n, 
                                            4, // idx_type 
                                            f, 
                                            mask);

    } else if constexpr (std::is_same_v<T, FloatT>) {

        apply_numeric_filter_idx<MapCol, 
                                 CORES, 
                                 NUMA, 
                                 IdxIsTrue>(dbl_v, 
                                            n, 
                                            5, // idx_type 
                                            f, 
                                            mask);

    } else if constexpr (std::is_same_v<T, CharT>) {

        apply_numeric_filter_idx<MapCol, 
                                 CORES, 
                                 NUMA, 
                                 IdxIsTrue>(dbl_v, 
                                            n, 
                                            1, // idx_type
                                            f, 
                                            mask);

    } else if constexpr (std::is_same_v<T, std::string>) {

        apply_numeric_filter_idx<MapCol, 
                                 CORES, 
                                 NUMA, 
                                 IdxIsTrue>(dbl_v, 
                                            n, 
                                            0,  // idx_type 
                                            f, 
                                            mask);

    }
}





