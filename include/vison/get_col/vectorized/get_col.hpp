#pragma once

template <typename T, bool IsBool = false>
void get_col(unsigned int &x, 
                std::vector<T> &rtn_v) {
  
    rtn_v.resize(nrow);
    unsigned int i;
    unsigned int i2 = 0;

    auto load_column = [&](auto& col_vec, const auto& idx_vec)
    {
        size_t idx = 0;
        while (idx < idx_vec.size() && x != idx_vec[idx])
            ++idx;
    
        if (idx == idx_vec.size()) {
            std::cerr << "Error in (get_col), no column found\n";
            return;
        }
    
        size_t offset = idx * nrow;
    
        #pragma GCC ivdep
        for (size_t r = 0; r < nrow; ++r)
            rtn_v[r] = col_vec[offset + r];
    };

    if constexpr (IsBool) {

      load_column(bool_v, matr_idx[2]);

    } else if constexpr (std::is_same_v<T, IntT>) {

      load_column(int_v, matr_idx[3]);

    } else if constexpr (std::is_same_v<T, UIntT>) {

      load_column(uint_v, matr_idx[4]);

    } else if constexpr (std::is_same_v<T, FloatT>) {

      load_column(dbl_v, matr_idx[5]);

    } else if constexpr (std::is_same_v<T, std::string>) {

      load_column(str_v, matr_idx[0]);

    } else if constexpr (std::is_same_v<T, CharT>) {

      load_column(chr_v, matr_idx[1]);

    } else {
      std::cerr << "Error in (get_col), unsupported type\n";
      return;
    };

};



