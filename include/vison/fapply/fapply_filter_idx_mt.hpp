#pragma once

template <unsigned int CORES           = 4,
          bool NUMA                    = false,
          bool IsBool                  = false,
          bool MapCol                  = false,
          bool IdxIsTrue               = true,
          bool Periodic                = true,
          AssertionType AssertionLevel = AssertionType::Simple,
          typename F>
requires FapplyFn<F, first_arg_t<F>>
void fapply_filter_idx_mt(F f, 
                          const unsigned int n, 
                          const std::vector<unsigned int>& mask) 
{

    const unsigned int local_nrow = nrow;

    if constexpr (AssertionLevel > AssertionType::None) {
        auto it = std::unique(mask.begin(), mask.end());
        if (it != mask.end()) {
            throw std::runtime_error("mask indices are not unique\n");
        }
    }

    if constexpr (AssertionLevel > AssertionType::Simple) {
        for (auto el : mask) {
            if (el >= local_nrow) {
                throw std::runtime_error("mask indices out of bounds\n");
            }
        }
    }

    using T = first_arg_t<F>;

    if constexpr (IsBool) {

        if constexpr (!(std::is_same_v<T, uint8_t>)) {
          std::cerr << "A bool must be uint8_t\n";
          return;
        }

        apply_numeric_filter_idx<MapCol, 
                                 CORES, 
                                 NUMA,
                                 IdxIsTrue,
                                 Periodic>(bool_v, 
                                           n, 
                                           2, // idx_type
                                           f, 
                                           mask);

    } else if constexpr (std::is_same_v<T, IntT>) {

        apply_numeric_filter_idx<MapCol, 
                                 CORES, 
                                 NUMA, 
                                 IdxIsTrue,
                                 Periodic>(int_v, 
                                           n, 
                                           3,  // idx_type 
                                           f, 
                                           mask);

    } else if constexpr (std::is_same_v<T, UIntT>) {

        apply_numeric_filter_idx<MapCol, 
                                 CORES, 
                                 NUMA, 
                                 IdxIsTrue,
                                 Periodic>(uint_v, 
                                           n, 
                                           4, // idx_type 
                                           f, 
                                           mask);

    } else if constexpr (std::is_same_v<T, FloatT>) {

        apply_numeric_filter_idx<MapCol, 
                                 CORES, 
                                 NUMA, 
                                 IdxIsTrue,
                                 Periodic>(dbl_v, 
                                           n, 
                                           5, // idx_type 
                                           f, 
                                           mask);

    } else if constexpr (std::is_same_v<T, CharT>) {

        apply_numeric_filter_idx<MapCol, 
                                 CORES, 
                                 NUMA, 
                                 IdxIsTrue,
                                 Periodic>(dbl_v, 
                                           n, 
                                           1, // idx_type
                                           f, 
                                           mask);

    } else if constexpr (std::is_same_v<T, std::string>) {

        apply_numeric_filter_idx<MapCol, 
                                 CORES, 
                                 NUMA, 
                                 IdxIsTrue,
                                 Periodic>(dbl_v, 
                                           n, 
                                           0,  // idx_type 
                                           f, 
                                           mask);

    }
}





