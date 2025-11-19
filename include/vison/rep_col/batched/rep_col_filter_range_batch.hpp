#pragma once

template <typename T,
          bool IsBool = false,
          size_t BATCH = 32>
void rep_col_filter_range_batch(std::vector<T>& x,
                                unsigned int& colnb,
                                const std::vector<uint8_t>& mask,
                                const unsigned int& strt_vl)
{
    if (x.size() != nrow) {
        std::cerr << "Error: vector length (" << x.size()
                  << ") does not match nrow (" << nrow << ")\n";
        return;
    }

    const size_t end_mask = mask.size();

    // -------------------------------------------------
    // helper: find col index and compute base offset
    // -------------------------------------------------
    auto find_col_base = [&](auto& idx_vec) -> size_t {
        size_t pos = 0;
        while (pos < idx_vec.size() && idx_vec[pos] != colnb)
            ++pos;
        if (pos == idx_vec.size()) {
            std::cerr << "Error: column " << colnb
                      << " not found for this type\n";
            return size_t(-1);
        }
        return pos * nrow;
    };

    // -------------------------------------------------
    // unified NUMERIC path (Bool / Int / UInt / Float)
    // fused copy + to_chars, with offsets
    // -------------------------------------------------
    auto replace_numeric = [&](auto& col_vec, auto& idx_vec)
    {
        constexpr size_t buf_size = max_chars_needed<T>();

        size_t base = find_col_base(idx_vec);
        if (base == size_t(-1)) return;

        T*         __restrict dst = col_vec.data() + base + strt_vl;
        const T*   __restrict src = x.data();

        std::vector<std::string>& val_tmp_full = tmp_val_refv[colnb];
        std::string* val_tmp = val_tmp_full.data() + strt_vl;

        for (size_t i = 0; i < end_mask; ++i)
            val_tmp[i].reserve(buf_size);

        alignas(64) char   local_bufs[BATCH][buf_size];
        uint8_t            lengths[BATCH];

        for (size_t i = 0; i < end_mask; i += BATCH) {
            size_t end = std::min(i + BATCH, end_mask);

            // fused: to_chars → copy → record length
            #pragma GCC ivdep
            for (size_t j = i; j < end; ++j) {
                if (!mask[j]) continue;
                dst[j] = src[j];
            }

            for (size_t j = i; j < end; ++j) {
                if (!mask[j]) continue;

                auto& cur_buf = local_bufs[j - i];

                auto [ptr, ec] = std::to_chars(
                    cur_buf,
                    cur_buf + buf_size,
                    src[j]
                );

                if (ec != std::errc{}) [[unlikely]]
                    std::terminate();

                lengths[j - i] = static_cast<uint8_t>(ptr - cur_buf);
            }

            // write strings
            for (size_t j = i; j < end; ++j) {
                if (!mask[j]) continue;

                auto& cur_buf = local_bufs[j - i];
                size_t len = lengths[j - i];
                val_tmp[j].assign(cur_buf, len);
            }
        }
    };

    // -------------------------------------------------
    // std::string path (offset-aware)
    // -------------------------------------------------
    auto replace_string = [&]()
    {
        size_t base = find_col_base(matr_idx[0]);
        if (base == size_t(-1)) return;

        auto*       __restrict dst = str_v.data() + base + strt_vl;
        const auto* __restrict src = x.data();

        // raw column copy
        #pragma unroll 32
        for (size_t i = 0; i < end_mask; ++i)
            if (mask[i]) dst[i] = src[i];

        std::vector<std::string>& val_tmp_full = tmp_val_refv[colnb];
        std::string* val_tmp = val_tmp_full.data() + strt_vl;

        alignas(64) std::string buf[BATCH];

        for (size_t i = 0; i < end_mask; i += BATCH) {
            size_t end = std::min(i + BATCH, end_mask);

            for (size_t j = i; j < end; ++j)
                if (mask[j]) buf[j - i] = src[j];

            #pragma unroll 32
            for (size_t j = i; j < end; ++j)
                if (mask[j]) val_tmp[j] = buf[j - i];
        }
    };

    // -------------------------------------------------
    // CharT path (fixed-size buffer) with offsets
    // -------------------------------------------------
    auto replace_charbuf = [&]()
    {
        size_t base = find_col_base(matr_idx[1]);
        if (base == size_t(-1)) return;

        CharT*       __restrict dst = chr_v.data() + base + strt_vl;
        const CharT* __restrict src = x.data();

        #pragma unroll 32
        for (size_t i = 0; i < end_mask; ++i)
            if (mask[i]) dst[i] = src[i];

        std::vector<std::string>& val_tmp_full = tmp_val_refv[colnb];
        std::string* val_tmp = val_tmp_full.data() + strt_vl;

        for (size_t i = 0; i < end_mask; ++i)
            val_tmp[i].reserve(df_charbuf_size);

        alignas(64) CharT buf[BATCH];

        for (size_t i = 0; i < end_mask; i += BATCH) {
            size_t end = std::min(i + BATCH, end_mask);

            for (size_t j = i; j < end; ++j)
                if (mask[j]) buf[j - i] = src[j];

            #pragma unroll 32
            for (size_t j = i; j < end; ++j)
                if (mask[j]) val_tmp[j].assign(buf[j - i],
                                               df_charbuf_size);
        }
    };

    // -------------------------------------------------
    // DISPATCH
    // -------------------------------------------------
    if constexpr (IsBool) {
        replace_numeric(bool_v, matr_idx[2]);

    } else if constexpr (std::is_same_v<T, IntT>) {
        replace_numeric(int_v, matr_idx[3]);

    } else if constexpr (std::is_same_v<T, UIntT>) {
        replace_numeric(uint_v, matr_idx[4]);

    } else if constexpr (std::is_same_v<T, FloatT>) {
        replace_numeric(dbl_v, matr_idx[5]);

    } else if constexpr (std::is_same_v<T, std::string>) {
        replace_string();

    } else if constexpr (std::is_same_v<T, CharT>) {
        replace_charbuf();

    } else {
        std::cerr << "Error unsupported type in (replace_col_filter_range)\n";
    }
}


