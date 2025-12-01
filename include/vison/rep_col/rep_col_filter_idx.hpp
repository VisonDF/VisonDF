#pragma once

template <typename T, bool IsBool = false> 
void rep_col_filter_idx(std::vector<T> &x, 
                        unsigned int &colnb,
                        const std::vector<unsigned int>& mask)
{
    if (x.size() != nrow) {
        std::cerr << "Error: vector length (" << x.size()
                  << ") does not match nrow (" << nrow << ")\n";
        return;
    }

    const unsigned int end_mask = mask.size();

    auto find_col_index = [&](auto& idxvec) -> size_t {
        for (size_t i = 0; i < idxvec.size(); ++i)
            if (idxvec[i] == colnb)
                return i;

        std::cerr << "Error: column " << colnb 
                  << " not found for this type in (replace_col)\n";
        return size_t(-1);
    };

    auto replace_numeric = [&](auto& column_vec, auto& idxvec) {
        using U = std::decay_t<decltype(column_vec[0])>;
        constexpr size_t buf_size = max_chars_needed<U>();

        size_t pos = find_col_index(idxvec);
        if (pos == size_t(-1)) return;

        U* dst = column_vec[pos].data();
        auto& val_tmp = tmp_val_refv[colnb];

        for (auto& s : val_tmp)
            s.reserve(buf_size);

        for (unsigned pos_idx : mask) {
            const U& v = x[pos_idx];
            dst[pos_idx] = v;

            char buf[buf_size];
            auto [ptr, ec] = fast_to_chars(buf, buf + buf_size, v);

            if (ec == std::errc{}) {
                val_tmp[pos_idx].assign(buf, ptr);
            } else {
                std::terminate();
            }
        }
    };

    auto replace_string = [&]() {
        size_t pos = find_col_index(matr_idx[0]);
        if (pos == size_t(-1)) return;

        std::string* dst = str_v[pos].data();
        auto& val_tmp = tmp_val_refv[colnb];

        for (unsigned pos_idx : mask) {
            dst[pos_idx] = x[pos_idx];
            val_tmp[pos_idx] = x[pos_idx];
        }
    };

    auto replace_charbuf = [&]() {
        size_t pos = find_col_index(matr_idx[1]);
        if (pos == size_t(-1)) return;

        CharT* dst = chr_v[pos].data();
        auto& val_tmp = tmp_val_refv[colnb];

        for (unsigned pos_idx : mask) {
            dst[pos_idx] = x[pos_idx];
            val_tmp[pos_idx].assign(x[pos_idx], df_charbuf_size);
        }
    };

    if constexpr (IsBool) {
        replace_numeric(bool_v,  matr_idx[2]);

    } else if constexpr (std::is_same_v<T, IntT>) {
        replace_numeric(int_v,   matr_idx[3]);

    } else if constexpr (std::is_same_v<T, UIntT>) {
        replace_numeric(uint_v,  matr_idx[4]);

    } else if constexpr (std::is_same_v<T, FloatT>) {
        replace_numeric(dbl_v,   matr_idx[5]);

    } else if constexpr (std::is_same_v<T, std::string>) {
        replace_string();

    } else if constexpr (std::is_same_v<T, CharT>) {
        replace_charbuf();

    } else {
        std::cerr << "Error unsupported type in (replace_col)\n";
    }
}


