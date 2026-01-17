#pragma once

template <unsigned int CORES           = 4,
          bool NUMA                    = false,
          bool IsBool                  = false,
          bool MapCol                  = false,
          bool OneIsTrue               = true,
          bool Periodic                = false,
          AssertionType AssertionLevel = AssertionType::Simple,
          typename F>
requires FapplyFn<F, first_arg_t<F>>
void fapply_filter_range_mt(F f, 
                            const unsigned int n, 
                            const std::vector<uint8_t>& mask,
                            const unsigned int strt_vl,
                            OffsetBoolMask& start_offset) 
{

    if constexpr (AssertionLevel > AssertionType::Simple) {
        if (strt_vl + mask.size() >= nrow)
            throw std::runtime_error("strt_vl + mask.size() >= nrow\n");
    }

    using T = first_arg_t<F>;

    static_assert(is_supported_type<T>, "return type not supported\n");

    if constexpr (IsBool) {

        if constexpr (!(std::is_same_v<T, uint8_t>)) {
          std::cerr << "A bool must be uint8_t\n";
          return;
        }

        apply_numeric_filter_boolmask<MapCol, 
                                      CORES, 
                                      NUMA,
                                      OneIsTrue,
                                      Periodic>(bool_v, 
                                                n, 
                                                2, 
                                                f, 
                                                mask,
                                                strt_vl,
                                                start_offset);

    } else if constexpr (std::is_same_v<T, IntT>) {

        apply_numeric_filter_boolmaks<MapCol, 
                                      CORES, 
                                      NUMA, 
                                      OneIsTrue,
                                      Periodic>(int_v, 
                                                n, 
                                                3, 
                                                f, 
                                                mask,
                                                strt_vl,
                                                start_offset);

    } else if constexpr (std::is_same_v<T, UIntT>) {

        apply_numeric_filter_boomask<MapCol, 
                                     CORES, 
                                     NUMA, 
                                     OneIsTrue,
                                     Periodic>(uint_v, 
                                               n, 
                                               4, 
                                               f, 
                                               mask,
                                               strt_vl,
                                               start_offset);

    } else if constexpr (std::is_same_v<T, FloatT>) {

        apply_numeric_filter_boolmask<MapCol, 
                                      CORES, 
                                      NUMA,
                                      OneIsTrue,
                                      Periodic>(dbl_v, 
                                                n, 
                                                5, 
                                                f, 
                                                mask,
                                                strt_vl,
                                                start_offset);

    } else if constexpr (std::is_same_v<T, CharT>) {

        apply_numeric_filter_boolmask<MapCol, 
                                      CORES, 
                                      NUMA, 
                                      OneIsTrue,
                                      Periodic>(dbl_v, 
                                                n, 
                                                1, 
                                                f, 
                                                mask,
                                                strt_vl,
                                                start_offset);

    } else if constexpr (std::is_same_v<T, std::string>) {

        apply_numeric_filter_boolmask<MapCol, 
                                      CORES, 
                                      NUMA,
                                      OneIsTrue,
                                      Periodic>(dbl_v, 
                                                n, 
                                                0, 
                                                f, 
                                                mask,
                                                strt_vl,
                                                start_offset);

    }

}



