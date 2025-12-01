#pragma once

template <typename T, bool IsBool = false> 
inline void rep_col_filter_boolmask(std::vector<T>& x, 
                          unsigned int& colnb,
                          const std::vector<uint8_t>& mask,
                          const unsigned int strt_vl)
{
    if (x.size() != nrow) {
        std::cerr << "Error: vector length (" << x.size()
                  << ") does not match nrow (" << nrow << ")\n";
        return;
    }

    const unsigned int end_mask = mask.size();

    auto find_col_index = [&](auto& idx_vec) -> size_t {
        size_t pos = 0;
        while (pos < idx_vec.size() && idx_vec[pos] != colnb)
            pos++;
        if (pos == idx_vec.size()) {
            std::cerr << "Error: column " << colnb
                      << " not found in (replace_col)\n";
            std::terminate();
        }
        return pos;
    };

    // ============================================================
    //  Unified typed column replacer (handles all types)
    // ============================================================
    auto replace_col = [&](auto& col_vec, auto& idx_vec)
    {
        using U = std::decay_t<decltype(col_vec[0])>;

        constexpr bool IsString   = std::is_same_v<U, std::string>;
        constexpr bool IsCharBuf  = std::is_array_v<U>;      // char[N]
        constexpr bool IsNumeric  = !IsString && !IsCharBuf;

        constexpr size_t buf_size =
            IsNumeric ? max_chars_needed<U>() : 0;

        // 1. Column lookup
        size_t pos = find_col_index(idx_vec);

        // 2. Data pointers (offset by strt_vl)
        U*      dst = col_vec[pos].data() + strt_vl;
        U*      src = x.data()            + strt_vl;
        auto&   vt  = tmp_val_refv[colnb];
        std::string* val_tmp = vt.data() + strt_vl;

        // 3. Reserve for mirror strings
        if constexpr (IsNumeric) {
            for (auto& s : vt)
                s.reserve(buf_size);
        } 
        else if constexpr (IsCharBuf) {
            for (auto& s : vt)
                s.reserve(df_charbuf_size);
        }

        // 4. Masked update
        for (size_t k = 0; k < end_mask; ++k)
        {
            if (!mask[k]) continue;

            const U& v = src[k];

            if constexpr (IsCharBuf) {
                dst[k] = v;
                val_tmp[k].assign(v, df_charbuf_size);
            } else if constexpr (IsString) {
                dst[k] = v;
                val_tmp[k] = v;
            } else if constexpr (IsNumeric) {
                dst[k] = v;

                char buf[buf_size];
                auto [ptr, ec] =
                    fast_to_chars(buf, buf + buf_size, v);

                if (ec == std::errc{}) {
                    val_tmp[k].assign(buf, ptr);
                } else {
                    std::terminate();
                }
            }
        }
    };

    if constexpr (IsBool)
        replace_col(bool_v, matr_idx[2]);

    else if constexpr (std::is_same_v<T, IntT>)
        replace_col(int_v, matr_idx[3]);

    else if constexpr (std::is_same_v<T, UIntT>)
        replace_col(uint_v, matr_idx[4]);

    else if constexpr (std::is_same_v<T, FloatT>)
        replace_col(dbl_v, matr_idx[5]);

    else if constexpr (std::is_same_v<T, std::string>)
        replace_col(str_v, matr_idx[0]);

    else if constexpr (std::is_same_v<T, CharT>)
        replace_col(chr_v, matr_idx[1]);

    else
        static_assert(!sizeof(T*), "Unsupported type");
}

