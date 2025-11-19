#pragma once

template <typename T,
          bool IsBool = false,
          unsigned int BATCH = 32>
void rep_col_filter_idx_batch(std::vector<T>& x,
                              unsigned int& colnb,
                              const std::vector<unsigned int>& mask)
{
    if (x.size() != nrow) {
        std::cerr << "Error: vector length (" << x.size()
                  << ") does not match nrow (" << nrow << ")\n";
        return;
    }

    const size_t end_mask = mask.size();

    // -------------------------------------------------
    // helper: find column index and return base offset
    // -------------------------------------------------
    auto find_col_base = [&](auto& idx_vec) -> size_t {
        size_t pos = 0;
        while (pos < idx_vec.size() && idx_vec[pos] != colnb)
            ++pos;

        if (pos == idx_vec.size()) {
            std::cerr << "Error: column " << colnb
                      << " not found for this type in (replace_col)\n";
            return size_t(-1);
        }
        return pos * nrow;
    };

    auto replace_numeric = [&](auto& col_vec, auto& idx_vec)
    {
        constexpr size_t buf_size = max_chars_needed<T>();

        size_t base = find_col_base(idx_vec);
        if (base == size_t(-1)) return;

        T*         __restrict dst = col_vec.data() + base;
        const T*   __restrict src = x.data();
        auto&      val_tmp        = tmp_val_refv[colnb];

        for (auto& el : val_tmp)
            el.reserve(buf_size);

        alignas(64) char   local_bufs[BATCH][buf_size];
        uint8_t            lengths[BATCH];

        for (size_t i = 0; i < end_mask; i += BATCH) {
            const size_t end = std::min(i + BATCH, end_mask);

            #pragma GCC ivdep
            for (size_t j = i; j < end; ++j) {
                const size_t pos_idx = mask[j];
                dst[pos_idx] = src[pos_idx];
            }

            for (size_t j = i; j < end; ++j) {
                const size_t pos_idx = mask[j];

                auto& cur_buf = local_bufs[j - i];
                auto [ptr, ec] = std::to_chars(
                    cur_buf,
                    cur_buf + buf_size,
                    src[pos_idx]
                );

                if (ec != std::errc{}) [[unlikely]]
                    std::terminate();

                lengths[j - i] = static_cast<uint8_t>(ptr - cur_buf);
            }

            for (size_t j = i; j < end; ++j) {
                const size_t pos_idx = mask[j];

                auto&      cur_buf = local_bufs[j - i];
                const auto len     = static_cast<size_t>(lengths[j - i]);
                auto&      s       = val_tmp[pos_idx];
                s.resize(len);
                std::memcpy(s.data(), cur_buf, len);
            }
        }
    };

    auto replace_string = [&]()
    {
        size_t base = find_col_base(matr_idx[0]);
        if (base == size_t(-1)) return;

        auto*       __restrict dst = str_v.data() + base;
        const auto* __restrict src = x.data();
        auto&       val_tmp        = tmp_val_refv[colnb];

        alignas(64) std::string buf[BATCH];

        for (size_t i = 0; i < end_mask; i += BATCH) {
            const size_t end = std::min(i + BATCH, end);

            // local copy in batch + dst write
            for (size_t j = i; j < end; ++j) {
                const size_t pos_idx = mask[j];
                const std::string& str_vl = src[pos_idx];
                dst[pos_idx] = str_vl;
                buf[j - i].assign(str_vl);
            }

            #pragma unroll 32
            for (size_t j = i; j < end; ++j) {
                const size_t pos_idx = mask[j];
                val_tmp[pos_idx].assign(buf[j - i]);
            }
        }
    };

    auto replace_charbuf = [&]()
    {
        size_t base = find_col_base(matr_idx[1]);
        if (base == size_t(-1)) return;

        CharT*       __restrict dst = chr_v.data() + base;
        const CharT* __restrict src = x.data();
        auto&        val_tmp        = tmp_val_refv[colnb];

        alignas(64) CharT buf[BATCH];

        for (size_t i = 0; i < end_mask; i += BATCH) {
            const size_t end = std::min(i + BATCH, end_mask);

            for (size_t j = i; j < end; ++j) {
                const size_t pos_idx = mask[j];
                const auto& vl = src[pos_idx];
                dst[pos_idx]  = vl;
                buf[j - i]    = vl;
            }

            #pragma unroll 32
            for (size_t j = i; j < end; ++j) {
                const size_t pos_idx = mask[j];
                val_tmp[pos_idx].assign(buf[j - i], df_charbuf_size);
            }
        }
    };

    if constexpr (IsBool) {
        replace_numeric(bool_v, matr_idx[2]);

    } else if constexpr (std::is_same_v<T, IntT>) {
        replace_numeric(int_v,  matr_idx[3]);

    } else if constexpr (std::is_same_v<T, UIntT>) {
        replace_numeric(uint_v, matr_idx[4]);

    } else if constexpr (std::is_same_v<T, FloatT>) {
        replace_numeric(dbl_v,  matr_idx[5]);

    } else if constexpr (std::is_same_v<T, std::string>) {
        replace_string();

    } else if constexpr (std::is_same_v<T, CharT>) {
        replace_charbuf();

    } else {
        std::cerr << "Error unsupported type in (replace_col)\n";
    }
}



