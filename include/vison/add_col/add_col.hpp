#pragma once

template <typename T, 
          bool Large = false, 
          bool BoolAsU8 = false> 
void add_col(std::vector<T> &x, std::string name = "NA") {
  
    if (x.size() != nrow) {
      std::cerr << "Error: vector length (" << x.size()
                << ") does not match nrow (" << nrow << ")\n";
      return;
    };
    
    name_v.push_back(name);
    tmp_val_refv.resize(tmp_val_refv.size() + 1);
    auto& val_tmp = tmp_val_refv.back();
    val_tmp.resize(nrow);

    auto reserve_for_numeric = [&]() {
        constexpr size_t buf_size = max_chars_needed<T>();
        for (auto& s : val_tmp) {
            s.reserve(buf_size);
        }
    };

    auto stringify_one = [&](size_t i, std::string* dst, const auto* src) {
        if constexpr (std::is_same_v<T, std::string>) {
            dst[i] = src[i];
        } else if constexpr (std::is_same_v<T, CharT>) {
            dst[i].assign(src[i], df_charbuf_size);  
        } else {
            constexpr size_t buf_size = max_chars_needed<T>();
            char buf[buf_size];
    
            auto [ptr, ec] = std::to_chars(buf, buf + buf_size, src[i]);
            if (ec == std::errc{}) [[likely]] {
                dst[i].assign(buf, ptr);
            } else [[unlikely]] {
                std::terminate();
            }
        }
    };
    
    auto stringify_loop = [&](size_t nrow, std::string* dst, const auto* src) {
        for (size_t i = 0; i < nrow; ++i) {
            stringify_one(i, dst, src);
        }
    };

    auto copy_column = [&](size_t nrow,
                           std::string* val_tmp_data,
                           auto* __restrict dst,
                           const auto* __restrict src) {
        if constexpr (Large) {
            #pragma GCC ivdep
            for (size_t i = 0; i < nrow; ++i) {
                dst[i] = src[i];
            }
            stringify_loop(nrow, val_tmp_data, src);
        } else {
            for (size_t i = 0; i < nrow; ++i) {
                dst[i] = src[i];
                stringify_one(i, val_tmp_data, src);
            }
        }
    };

    if constexpr (BoolAsU8) {

      matr_idx[2].push_back(ncol);
      type_refv.push_back('b');
      
      const size_t base_idx = bool_v.size();
      bool_v.resize(base_idx + nrow);

      auto* __restrict dst = std::assume_aligned<64>(bool_v.data() + base_idx);
      auto* __restrict src = std::assume_aligned<64>(x.data());

      reserve_for_numeric();
      copy_column(nrow, val_tmp.data(), dst, src);

    } else if constexpr (std::is_same_v<T, IntT>) {

      matr_idx[3].push_back(ncol);
      type_refv.push_back('i');

      const size_t base_idx = int_v.size();
      int_v.resize(base_idx + nrow);
    
      auto* __restrict dst = std::assume_aligned<64>(int_v.data() + base_idx);
      auto* __restrict src = std::assume_aligned<64>(x.data());

      reserve_for_numeric();
      copy_column(nrow, val_tmp.data(), dst, src);

    } else if constexpr (std::is_same_v<T, UIntT>) {

      matr_idx[4].push_back(ncol);
      type_refv.push_back('u');
 
      const size_t base_idx = uint_v.size();
      uint_v.resize(base_idx + nrow);
    
      auto* __restrict dst = std::assume_aligned<64>(uint_v.data() + base_idx);
      auto* __restrict src = std::assume_aligned<64>(x.data());

      reserve_for_numeric();
      copy_column(nrow, val_tmp.data(), dst, src);

    } else if constexpr (std::is_same_v<T, FloatT>) {

      matr_idx[5].push_back(ncol);
      type_refv.push_back('d');

      const size_t base_idx = dbl_v.size();
      dbl_v.resize(base_idx + nrow);
    
      auto* __restrict dst = std::assume_aligned<64>(dbl_v.data() + base_idx);
      auto* __restrict src = std::assume_aligned<64>(x.data());

      reserve_for_numeric();
      copy_column(nrow, val_tmp.data(), dst, src);

    } else if constexpr (std::is_same_v<T, CharT>) {

      matr_idx[1].push_back(ncol);
      type_refv.push_back('c');

      const size_t base_idx = chr_v.size();
      chr_v.resize(base_idx + nrow);
     
      auto* __restrict dst = std::assume_aligned<64>(chr_v.data() + base_idx);
      auto* __restrict src = std::assume_aligned<64>(x.data());

      for (auto& s : val_tmp) {
          s.reserve(df_charbuf_size);
      }
      copy_column(nrow, val_tmp.data(), dst, src);

    } else if constexpr (std::is_same_v<T, std::string>) {

      matr_idx[0].push_back(ncol);
      type_refv.push_back('s');
     
      const size_t base_idx = str_v.size();
      str_v.resize(base_idx + nrow);

      auto* __restrict dst = std::assume_aligned<64>(str_v.data() + base_idx);
      auto* __restrict src = std::assume_aligned<64>(x.data());

      copy_column(nrow, val_tmp.data(), dst, src);

    } else {
      std::cerr << "Error in (add_col) type not suported \n";
    };

    ncol += 1;
};



