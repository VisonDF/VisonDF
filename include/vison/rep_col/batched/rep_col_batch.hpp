#pragma once

template <typename T,
          bool IsBool = false,
          size_t BATCH = 32>
void rep_col_batch(std::vector<T>& x, unsigned int& colnb)
{
    static_assert(BATCH > 0, "BATCH must be > 0");

    const unsigned int local_nrow = nrow;

    if (x.size() != local_nrow) {
        std::cerr << "Error: vector length (" << x.size()
                  << ") does not match nrow (" << nrow << ")\n";
        return;
    }

    auto find_col_base = [&](auto& idx_vec) -> size_t {
        size_t pos = 0;
        while (pos < idx_vec.size() && idx_vec[pos] != colnb)
            ++pos;

        if (pos == idx_vec.size()) {
            std::cerr << "Error: column " << colnb
                      << " not found for this type in (replace_col)\n";
            return size_t(-1);
        }
        return pos;
    };

    auto replace_numeric = [&](auto& col_vec, auto& idx_vec)
    {
        constexpr size_t buf_size = max_chars_needed<T>();

        size_t pos = find_col_base(idx_vec);
        if (pos == size_t(-1)) return;

        T*          __restrict dst = col_vec[pos].data();
        const T*    __restrict src = x.data();
        auto&       val_tmp        = tmp_val_refv[colnb];

        for (auto& s : val_tmp)
            s.reserve(buf_size);

        alignas(64) char   local_bufs[BATCH][buf_size];
        uint8_t            lengths[BATCH];

        for (size_t i = 0; i < local_nrow; i += BATCH) {
            const size_t end = std::min(i + BATCH, static_cast<size_t>(local_nrow));

            memcpy(dst + i, src + i, (end - i) * sizeof(IntT));

            for (size_t j = i; j < end; ++j) {
                auto& cur_buf = local_bufs[j - i];
                auto [ptr, ec] = std::to_chars(cur_buf,
                                               cur_buf + buf_size,
                                               src[j]);
                if (ec != std::errc{}) [[unlikely]]
                    std::terminate();
                lengths[j - i] = static_cast<uint8_t>(ptr - cur_buf);
            }

            for (size_t j = i; j < end; ++j) {
                auto&      cur_buf = local_bufs[j - i];
                const auto len     = static_cast<size_t>(lengths[j - i]);
                auto&      s       = val_tmp[j];
                s.resize(len);
                std::memcpy(s.data(), cur_buf, len);
            }
        }
    };

    auto replace_string = [&]()
    {
        size_t pos = find_col_base(matr_idx[0]);
        if (pos == size_t(-1)) return;

        auto*       __restrict dst = str_v[pos].data();
        const auto* __restrict src = x.data();

        // write underlying column
        #pragma unroll 32
        for (size_t i = 0; i < local_nrow; ++i)
            dst[i] = src[i];

        std::vector<std::string>& __restrict val_tmp = tmp_val_refv[colnb];

        alignas(64) std::string buf[BATCH];

        for (size_t i = 0; i < local_nrow; i += BATCH) {
            const size_t end = std::min(i + BATCH, static_cast<size_t>(local_nrow));

            // local copy in batch
            for (size_t j = i; j < end; ++j)
                buf[j - i].assign(src[j]);

            // write back to val_tmp
            #pragma unroll 32
            for (size_t j = i; j < end; ++j)
                val_tmp[j].assign(buf[j - i]);
        }
    };

    auto replace_charbuf = [&]()
    {
        size_t pos = find_col_base(matr_idx[1]);
        if (pos == size_t(-1)) return;

        CharT*       __restrict dst = chr_v[pos].data();
        const CharT* __restrict src = x.data();

        memcpy(dst,
               src,
               local_nrow * sizeof(CharT));

        std::vector<std::string>& __restrict val_tmp = tmp_val_refv[colnb];

        for (auto& s : val_tmp)
            s.reserve(df_charbuf_size);

        alignas(64) CharT buf[BATCH];

        for (size_t i = 0; i < local_nrow; i += BATCH) {
            const size_t end = std::min(i + BATCH, static_cast<size_t>(local_nrow));

            // local cache of buffers
            for (size_t j = i; j < end; ++j)
                buf[j - i] = src[j];

            // assign each fixed buffer into string
            #pragma unroll 32
            for (size_t j = i; j < end; ++j)
                val_tmp[j].assign(buf[j - i], df_charbuf_size);
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


