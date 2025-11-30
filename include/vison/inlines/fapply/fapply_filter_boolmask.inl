#pragma once

template <typename F, bool IsBool = false>
inline void fapply_filter_boolmask(F f, 
                            unsigned int& n, 
                            const std::vector<uint8_t>& mask,
                            const unsigned int strt_vl) 
{

    assert(mask.size() <= nrow);

    using T = first_arg_t<F>;

    if constexpr (IsBool) {
        
        if constexpr (!(std::is_same_v<T, uint8_t>)) {
          std::cerr << "A bool must be uint8_t\n";
          return;
        }
        
        apply_numeric_filter_range<uint8_t>(bool_v, 
                                            n, 
                                            0, 
                                            f, 
                                            mask,
                                            strt_vl);

    } else if constexpr (std::is_same_v<T, IntT>)
        apply_numeric_filter_range<IntT>(int_v, 
                                         n, 
                                         3, 
                                         f, 
                                         mask,
                                         strt_vl);

    else if constexpr (std::is_same_v<T, UIntT>)
        apply_numeric_filter_range<UIntT>(uint_v, 
                                          n, 
                                          4, 
                                          f, 
                                          mask,
                                          strt_vl);

    else if constexpr (std::is_same_v<T, FloatT>)
        apply_numeric_filter_range<FloatT>(dbl_v, 
                                           n, 
                                           5, 
                                           f, 
                                           mask,
                                           strt_vl);

    else if constexpr (std::is_same_v<T, CharT>) {
        unsigned int i2 = 0;
        while (i2 < matr_idx[1].size() && n != matr_idx[1][i2])
            ++i2;

        std::vector<CharT>& dst = chr_v[i2];
        std::vector<std::string>& val_tmp = tmp_val_refv[n];
        const unsigned int end_val = mask.size();
        for (size_t i = 0; i < end_val; ++i) {
            if (!mask[i]) { continue; }
            f(dst[strt_vl + i]);
            val_tmp[strt_vl + i].assign(dst[strt_vl + i], df_charbuf_size);
        }
    }

    else if constexpr (std::is_same_v<T, std::string>) {
        unsigned int i2 = 0;
        while (i2 < matr_idx[0].size() && n != matr_idx[0][i2])
            ++i2;

        std::vector<std::string>& dst = str_v[i2];
        std::vector<std::string>& val_tmp = tmp_val_refv[n]; 
        const unsigned int end_val = mask.size();
        for (size_t i = 0; i < end_val; ++i) {
            if (!mask[i]) { continue; }
            f(dst[strt_vl + i]);
            val_tmp[strt_vl + i] = dst[strt_vl + i];
        }
    }
}


