#pragma once

template <typename T, bool IsBool = false>
void rep_col_filter(std::vector<T>& x,
                    unsigned int& colnb,
                    const std::vector<uint8_t>& mask)
{
    if (x.size() != nrow) {
        std::cerr << "Error: vector length (" << x.size()
                  << ") does not match nrow (" << nrow << ")\n";
        return;
    }

    const size_t end_mask = mask.size();

    auto find_col_index = [&](auto& idxvec) -> size_t {
        size_t pos = 0;
        while (pos < idxvec.size() && idxvec[pos] != colnb)
            ++pos;

        if (pos == idxvec.size()) {
            std::cerr << "Error: column " << colnb << " not found in (replace_col)\n";
            return size_t(-1);
        }
        return pos;
    };

    auto replace_col = [&](auto& col_vec, auto& idx_vec)
    {
        using U = std::decay_t<decltype(col_vec[0])>;

        constexpr bool IsString  = std::is_same_v<U, std::string>;
        constexpr bool IsCharBuf = std::is_array_v<U>;      // char[N]
        constexpr bool IsNumeric = !IsString && !IsCharBuf;

        constexpr size_t BUF_SIZE =
            IsNumeric ? max_chars_needed<U>() :
            IsCharBuf ? df_charbuf_size : 0;

        size_t pos = find_col_index(idx_vec);
        if (pos == size_t(-1)) return;

        size_t base = pos * nrow;

        U* dst = col_vec.data() + base;
        U* src = x.data();
        auto& val_tmp_full = tmp_val_refv[colnb];
        std::string* val_tmp = val_tmp_full.data();

        // Reserve once
        if constexpr (IsNumeric || IsCharBuf)
            for (auto& s : val_tmp_full)
                s.reserve(BUF_SIZE);

        // Masked write
        for (size_t i = 0; i < end_mask; ++i)
        {
            if (!mask[i]) continue;

            const U& v = src[i];

            if constexpr (IsString)
            {
                dst[i] = v;
                val_tmp[i] = v;
            }
            else if constexpr (IsCharBuf)
            {
                dst[i] = v;                          
                val_tmp[i].assign(v, df_charbuf_size);
            }
            else if constexpr (IsNumeric)
            {
                dst[i] = v;

                char buf[BUF_SIZE];
                auto [ptr, ec] =
                    std::to_chars(buf, buf + BUF_SIZE, v);   

                if (ec == std::errc{}) [[likely]]
                    val_tmp[i].assign(buf, ptr);
                else [[unlikely]]
                    std::terminate();
            }
        }
    };

    // --- Dispatch by T (NOT by IsBool) ---
    if constexpr (IsBool)
        replace_col(bool_v, matr_idx[2]);

    else if constexpr (std::is_same_v<T, IntT>)
        replace_col(int_v,  matr_idx[3]);

    else if constexpr (std::is_same_v<T, UIntT>)
        replace_col(uint_v, matr_idx[4]);

    else if constexpr (std::is_same_v<T, FloatT>)
        replace_col(dbl_v,  matr_idx[5]);

    else if constexpr (std::is_same_v<T, std::string>)
        replace_col(str_v,  matr_idx[0]);

    else if constexpr (std::is_same_v<T, CharT>)
        replace_col(chr_v,  matr_idx[1]);

    else
        static_assert(!sizeof(T*), "Unsupported type");
}


