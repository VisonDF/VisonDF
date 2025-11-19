#pragma once

template <typename T, bool IsBool = false> 
void rep_col_filter(std::vector<T> &x, 
                unsigned int &colnb,
                const std::vector<uint8_t>& mask) {

    if (x.size() != nrow) {
      std::cerr << "Error: vector length (" << x.size()
                << ") does not match nrow (" << nrow << ")\n";
      return;
    }
 
    const unsigned int end_mask = mask.size();

    auto find_col_index = [&](auto& vec) -> size_t {
        size_t pos = 0;
        while (pos < vec.size() && vec[pos] != colnb)
            ++pos;
        if (pos == vec.size()) {
            std::cerr << "Error: column " << colnb << " not found in (replace_col)\n";
            return size_t(-1);
        }
        return pos;
    };

    auto replace_numeric = [&](auto& column_vec, auto& idxvec) {
    
        using U = std::decay_t<decltype(column_vec[0])>;
        constexpr size_t buf_size = max_chars_needed<U>();
    
        size_t pos = find_col_index(idxvec);
        if (pos == size_t(-1)) return;
    
        size_t base = pos * nrow;
        U* dst = column_vec.data() + base;
        auto& val_tmp = tmp_val_refv[colnb];
    
        // reserve output strings
        for (auto& s : val_tmp)
            s.reserve(buf_size);
    
        // masked assignment
        for (size_t i = 0; i < end_mask; ++i) {
    
            if (!mask[i]) continue;
    
            const U& v = x[i];
            dst[i] = v;
    
            char buf[buf_size];
            auto [ptr, ec] = std::to_chars(buf, buf + buf_size, v);
    
            if (ec == std::errc{}) [[likely]] {
                val_tmp[i].assign(buf, ptr);
            } else [[unlikely]] {
                std::terminate();
            }
        }
    };

    auto replace_string = [&]() {
    
        size_t pos = find_col_index(matr_idx[0]);
        if (pos == size_t(-1)) return;
    
        size_t base = pos * nrow;
        std::string* dst = str_v.data() + base;
        auto& val_tmp = tmp_val_refv[colnb];
    
        for (size_t i = 0; i < end_mask; ++i) {
            if (!mask[i]) continue;
    
            const std::string& v = x[i];
            dst[i] = v;
            val_tmp[i] = v;
        }
    };

    auto replace_charbuf = [&]() {
    
        size_t pos = find_col_index(matr_idx[1]);
        if (pos == size_t(-1)) return;
    
        size_t base = pos * nrow;
        CharT* dst = chr_v.data() + base;
        auto& val_tmp = tmp_val_refv[colnb];
    
        for (size_t i = 0; i < end_mask; ++i) {
            if (!mask[i]) continue;
    
            dst[i] = x[i];
            val_tmp[i].assign(x[i], df_charbuf_size);
        }
    };

    if constexpr (IsBool)
        replace_numeric(bool_v, matr_idx[2]);
    
    else if constexpr (std::is_same_v<T, IntT>)
        replace_numeric(int_v, matr_idx[3]);
    
    else if constexpr (std::is_same_v<T, UIntT>)
        replace_numeric(uint_v, matr_idx[4]);
    
    else if constexpr (std::is_same_v<T, FloatT>)
        replace_numeric(dbl_v, matr_idx[5]);
    
    else if constexpr (std::is_same_v<T, std::string>)
        replace_string();
    
    else if constexpr (std::is_same_v<T, CharT>)
        replace_charbuf();
    
    else
        static_assert(!sizeof(T*), "Unsupported type");

};




