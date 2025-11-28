#pragma once

template <typename T, bool IsBool = false>
void get_col(unsigned int &x, 
             std::vector<T> &rtn_v) {
  
    rtn_v.resize(nrow);

    auto find_col_base = [&](auto &idx_vec) -> size_t {
        size_t pos = 0;
        while (pos < idx_vec.size() && idx_vec[pos] != x)
            ++pos;

        if (pos == idx_vec.size()) {
            std::cerr << "Error in (get_col), no column found\n";
            return size_t(-1);
        }
        return pos;
    };

    auto load_column = [&](const auto *__restrict src)
    {
        memcpy(rtn_v.data(), 
               src, 
               nrow * sizeof(T));
    };

    if constexpr (std::is_same_v<T, std::string>) {
      const size_t pos_base = find_col_base(matr_idx[0]);
      const auto& src = str_v[pos_base];
      for (size_t i = 0; i < nrow; ++i)
          rtn_v[i] = src[i];

    } else if constexpr (std::is_same_v<T, CharT>) {
      const size_t pos_base = find_col_base(matr_idx[1]);
      load_column(chr_v[pos_base].data());

    } else if constexpr (IsBool) {
      const size_t pos_base = find_col_base(matr_idx[2]);
      load_column(bool_v[pos_base].data());

    } else if constexpr (std::is_same_v<T, IntT>) {
      const size_t pos_base = find_col_base(matr_idx[3]);
      load_column(int_v[pos_base].data());

    } else if constexpr (std::is_same_v<T, UIntT>) {
      const size_t pos_base = find_col_base(matr_idx[4]);
      load_column(uint_v[pos_base].data());

    } else if constexpr (std::is_same_v<T, FloatT>) {
      const size_t pos_base = find_col_base(matr_idx[5]);
      load_column(uint_v[pos_base].data());

    } else {
      std::cerr << "Error in (get_col), unsupported type\n";
      return;
    };

};



