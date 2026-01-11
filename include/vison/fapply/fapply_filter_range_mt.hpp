#pragma once

template <unsigned int CORES = 4,
          bool NUMA = false,
          bool IsBool = false,
          bool MapCol = false,
          bool OneIsTrue = true,
          typename F>
requires FapplyFn<F, first_arg_t<F>>
void fapply_filter_range_mt(F f, 
                            const unsigned int n, 
                            const std::vector<uint8_t>& mask,
                            const unsigned int strt_vl,
                            OffsetBoolMask& start_offset) 
{

    using T = first_arg_t<F>;

    const unsigned int end = mask.size();

    if constexpr (IsBool) {

        if constexpr (!(std::is_same_v<T, uint8_t>)) {
          std::cerr << "A bool must be uint8_t\n";
          return;
        }

        apply_numeric_filter_boolmask<MapCol, 
                                      CORES, 
                                      NUMA,
                                      OneIsTrue>(bool_v, 
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
                                      OneIsTrue>(int_v, 
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
                                     OneIsTrue>(uint_v, 
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
                                      OneIsTrue>(dbl_v, 
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
                                      OneIsTrue>(dbl_v, 
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
                                      OneIsTrue>(dbl_v, 
                                                 n, 
                                                 0, 
                                                 f, 
                                                 mask,
                                                 strt_vl,
                                                 start_offset);

    }

}



