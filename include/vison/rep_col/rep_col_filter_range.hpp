#pragma once

template <typename T, bool IsBool = false> 
void rep_col_filter_range(std::vector<T> &x, 
                unsigned int &colnb,
                const std::vector<uint8_t>& mask,
                const unsigned int& strt_vl) {

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
                      << " not found for this type in (replace_col)\n";
            std::terminate();
        }
        return pos;
    };

    auto replace_numeric = [&](auto& column_vec, auto& idxvec) 
    {

        using U = std::decay_t<decltype(column_vec[0])>;
        constexpr size_t buf_size = max_chars_needed<U>();
    
        size_t pos = find_col_index(idxvec);
        if (pos == size_t(-1)) return;
    
        size_t base = pos * nrow;
        U* dst = column_vec.data() + base + strt_vl;
        U* src = x.data() + strt_vl;

        auto& vt_full = tmp_val_refv[colnb];
        std::string* val_tmp = vt_full.data() + strt_vl;
    
        for (auto& s : vt_full)
            s.reserve(buf_size);
    
        for (size_t k = 0; k < end_mask; ++k) {
            
            if (!mask[k])
                continue;
    
            const U& v = src[k];
    
            dst[k] = v;
    
            char buf[buf_size];
            auto [ptr, ec] = std::to_chars(buf, buf + buf_size, v);
    
            if (ec == std::errc{}) [[likely]] {
                val_tmp[k].assign(buf, ptr);
            } else [[unlikely]] {
                std::terminate();
            }
        }
    };

    auto replace_string = [&]() {
    
        size_t pos = find_col_index(matr_idx[0]);
        if (pos == size_t(-1)) return;
    
        size_t base = pos * nrow;
        std::string* dst = str_v.data() + base + strt_vl;
        std::string* src = x.data() + strt_vl;
        std::string* val_tmp = tmp_val_refv[colnb].data() + strt_vl;
    
        for (size_t k = 0; k < end_mask; ++k) {
            if (!mask[k]) continue;
    
            const std::string& v = src[k];
            dst[k] = v;
            val_tmp[k] = v;
        }
    };

    auto replace_charbuf = [&]() {
    
        size_t pos = find_col_index(matr_idx[1]);
        if (pos == size_t(-1)) return;
    
        size_t base = pos * nrow;
        CharT* dst = chr_v.data() + base + strt_vl;
        CharT* src = x.data() + strt_vl;
        auto& vt_full = tmp_val_refv[colnb];
        std::string* val_tmp = vt_full.data() + strt_vl;

        for (auto& el : vt_full) {
          el.reserve(df_charbuf_size);
        }

        for (size_t i = 0; i < end_mask; ++i) {
            if (!mask[i]) continue;
    
            dst[i] = src[i];
            val_tmp[i].assign(src[i], df_charbuf_size);
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




