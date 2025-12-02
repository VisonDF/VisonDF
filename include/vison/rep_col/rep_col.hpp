#pragma once

template <typename T, bool Large = false, bool IsBool = false> 
void rep_col(std::vector<T> &x, unsigned int &colnb) {

    const unsigned int local_nrow = nrow;

    if (x.size() != local_nrow) {
      std::cerr << "Error: vector length (" << x.size()
                << ") does not match nrow (" << nrow << ")\n";
      return;
    }
 
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
        //size_t base = pos * nrow;
        U* dst = col_vec[pos].data();
    
        auto& val_tmp = tmp_val_refv[colnb];
        for (auto& s: val_tmp)
            s.reserve(buf_size);
    
        memcpy(dst, 
               x.data(), 
               local_nrow * sizeof(T));
    
        // convert x[i] → string view
        for (size_t i = 0; i < local_nrow; ++i) {
            char buf[buf_size];
            auto [ptr, ec] = fast_to_chars(buf, buf + buf_size, x[i]);
            if (ec == std::errc{}) {
                val_tmp[i].assign(buf, ptr);
            } else {
                std::terminate();
            }
        }
    };

    auto replace_string = [&](){
    
        size_t pos = find_col_index(matr_idx[0]);
        std::string* dst = str_v[pos].data();
        auto& val_tmp = tmp_val_refv[colnb];
    
        if constexpr (Large) {
            for (size_t i = 0; i < local_nrow; ++i)
                dst[i] = x[i];
            for (size_t i = 0; i < local_nrow; ++i)
                val_tmp[i] = x[i];
        } else {
            for (size_t i = 0; i < local_nrow; ++i) {
                dst[i] = x[i];
                val_tmp[i] = x[i];
            }
        }
    };

    auto replace_charbuf = [&]() {
    
        size_t pos = find_col_index(matr_idx[1]);
        CharT* dst = chr_v[pos].data();
        auto& val_tmp = tmp_val_refv[colnb];
    
        if constexpr (!Large) { 
            for (size_t i = 0; i < local_nrow; ++i) {
                dst[i] = x[i]; 
                val_tmp[i].assign(x[i], df_charbuf_size);
            }
        } else {
            memcpy(dst, 
                   x.data(),
                   local_nrow * sizeof(CharT);
            for (size_t i = 0; i < local_nrow; ++i)
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




