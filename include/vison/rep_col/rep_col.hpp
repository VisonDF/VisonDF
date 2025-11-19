#pragma once

template <typename T, bool Large = false, bool IsBool = false> 
void rep_col(std::vector<T> &x, unsigned int &colnb) {

    if (x.size() != nrow) {
      std::cerr << "Error: vector length (" << x.size()
                << ") does not match nrow (" << nrow << ")\n";
      return;
    }
 
    unsigned int i;
    unsigned int i2 = 0;

    auto find_col_index = [&](auto& idx_vec) -> size_t {
        size_t pos = 0;
        while (pos < idx_vec.size() && idx_vec[pos] != colnb)
            pos++;
        if (pos == idx_vec.size()) {
            std::cerr << "Error: column " << colnb
                      << " not found for this type in (replace_col)\n";
            std::terminate();
        }
        return pos;
    };

    auto replace_numeric = [&](auto& col_vec, auto& idx_vec) {
    
        using U = std::decay_t<decltype(col_vec[0])>;     // column type
        constexpr size_t buf_size = max_chars_needed<U>();
    
        size_t pos  = find_col_index(idx_vec);
        size_t base = pos * nrow;
    
        auto& val_tmp = tmp_val_refv[colnb];
        for (auto& s: val_tmp)
            s.reserve(buf_size);
    
        // write raw values
        if constexpr (Large) {
            for (size_t i = 0; i < nrow; ++i)
                col_vec[base + i] = x[i];
        } else {
            for (size_t i = 0; i < nrow; ++i) {
                auto& v = x[i];
                col_vec[base + i] = v;
            }
        }
    
        // convert x[i] → string view
        for (size_t i = 0; i < nrow; ++i) {
            char buf[buf_size];
            auto [ptr, ec] = std::to_chars(buf, buf + buf_size, x[i]);
            if (ec == std::errc{}) {
                val_tmp[i].assign(buf, ptr);
            } else {
                std::terminate();
            }
        }
    };

    auto replace_string = [&](){
    
        size_t pos = find_col_index(matr_idx[0]);
        size_t base = pos * nrow;
        auto& val_tmp = tmp_val_refv[colnb];
    
        if constexpr (Large) {
            for (size_t i = 0; i < nrow; ++i)
                str_v[base + i] = x[i];
            for (size_t i = 0; i < nrow; ++i)
                val_tmp[i] = x[i];
        } else {
            for (size_t i = 0; i < nrow; ++i) {
                str_v[base + i] = x[i];
                val_tmp[i]       = x[i];
            }
        }
    };

    auto replace_charbuf = [&]() {
    
        size_t pos = find_col_index(matr_idx[1]);
        size_t base = pos * nrow;
        auto& val_tmp = tmp_val_refv[colnb];
    
        if constexpr (!Large) {
    
            for (size_t i = 0; i < nrow; ++i) {
                chr_v[base + i] = x[i];
    
                val_tmp[i].assign(x[i], df_charbuf_size);
            }
    
        } else {
    
            // First pass: update underlying column data
            for (size_t i = 0; i < nrow; ++i)
                chr_v[base + i] = x[i];
    
            // Second pass: update temporary string values
            for (size_t i = 0; i < nrow; ++i)
                val_tmp[i].assign(x[i], df_charbuf_size);
        }
    };

    if constexpr (IsBool) {

        replace_numeric(bool_v,  matr_idx[2]);

    } else if constexpr (std::is_same_v<T, IntT>) {

        replace_numeric(int_v,  matr_idx[3]);

    } else if constexpr (std::is_same_v<T, UIntT>) {

        replace_numeric(uint_v,  matr_idx[4]);

    } else if constexpr (std::is_same_v<T, FloatT>) {

        replace_numeric(dbl_v,  matr_idx[5]);

    } else if constexpr (std::is_same_v<T, std::string>) {

        replace_string();

    } else if constexpr (std::is_same_v<T, CharT>) {

        replace_charbuf();

    } else {
      std::cerr << "Error unsupported type in (replace_col)\n";
    };
};




