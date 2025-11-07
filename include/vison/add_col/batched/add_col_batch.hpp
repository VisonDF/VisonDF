#pragma once

template <typename T>
void add_col_batch(std::vector<T> &x, std::string name = "NA") {
  
    if (x.size() != nrow) {
      std::cerr << "Error: vector length (" << x.size()
                << ") does not match nrow (" << nrow << ")\n";
      return;
    };
    
    name_v.push_back(name);
    tmp_val_refv.resize(tmp_val_refv.size() + 1);
    auto& val_tmp = tmp_val_refv.back();
    val_tmp.resize(nrow);

    if constexpr (std::is_same_v<T, bool>) {

      matr_idx[2].push_back(ncol);
      type_refv.push_back('b');
      
      const size_t base_idx = bool_v.size();
      bool_v.resize(base_idx + nrow);
    
      const size_t buf_size = 2;
      size_t i;

      for (auto& el : val_tmp) {
        el.reserve(buf_size);
      }

      constexpr size_t BATCH = 256;
      
      for (size_t i = 0; i < nrow; i += BATCH) {
          const size_t end = std::min(i + BATCH, static_cast<size_t>(nrow));
      
          #pragma GCC unroll 8
          for (size_t k = i; k < end; ++k) {
              const bool b = x[k];
              bool_v[base_idx + k] = b;
      
              char buf[buf_size];
              auto [ptr, ec] = std::to_chars(buf, buf + buf_size,
                                             static_cast<int>(b));
              if (ec != std::errc{}) [[unlikely]]
                  std::terminate();
      
              val_tmp[k].assign(buf, ptr);
          }
      }

    } else if constexpr (std::is_same_v<T, IntT>) {

      matr_idx[3].push_back(ncol);
      type_refv.push_back('i');

      const size_t base_idx = int_v.size();
      int_v.resize(base_idx + nrow);
    
      constexpr size_t buf_size = max_chars_needed<T>();
      
      auto* __restrict dst = std::assume_aligned<64>(int_v.data() + base_idx);
      auto* __restrict src = std::assume_aligned<64>(x.data());

      std::memcpy(dst, src, nrow * sizeof(T));  
      
      for (auto& s : val_tmp)
          s.reserve(buf_size);                  
      
      constexpr size_t BATCH = 256;
      for (size_t i = 0; i < nrow; i += BATCH) {
          const size_t end = std::min(static_cast<size_t>(nrow), i + BATCH);
          alignas(64) char bufs[BATCH][buf_size];  
      
          #pragma GCC unroll 8
          for (size_t k = 0; k < end - i; ++k) {
              auto [ptr, ec] = std::to_chars(bufs[k], bufs[k] + buf_size, dst[i + k]);
              if (ec != std::errc{}) [[unlikely]] std::terminate();
              val_tmp[i + k].assign(bufs[k], ptr);
          }
      }
      
    } else if constexpr (std::is_same_v<T, UIntT>) {

      matr_idx[4].push_back(ncol);
      type_refv.push_back('u');
 
      const size_t base_idx = uint_v.size();
      uint_v.resize(base_idx + nrow);
    
      constexpr size_t buf_size = max_chars_needed<T>();
      
      auto* __restrict dst = std::assume_aligned<64>(uint_v.data() + base_idx);
      auto* __restrict src = std::assume_aligned<64>(x.data());

      std::memcpy(dst, src, nrow * sizeof(T));  
      
      for (auto& s : val_tmp)
          s.reserve(buf_size);                  
      
      constexpr size_t BATCH = 256;
      for (size_t i = 0; i < nrow; i += BATCH) {
          const size_t end = std::min(static_cast<size_t>(nrow), i + BATCH);
          alignas(64) char bufs[BATCH][buf_size];  
      
          #pragma GCC unroll 8
          for (size_t k = 0; k < end - i; ++k) {
              auto [ptr, ec] = std::to_chars(bufs[k], bufs[k] + buf_size, dst[i + k]);
              if (ec != std::errc{}) [[unlikely]] std::terminate();
              val_tmp[i + k].assign(bufs[k], ptr);
          }
      }
      
    } else if constexpr (std::is_same_v<T, FloatT>) {

      matr_idx[5].push_back(ncol);
      type_refv.push_back('d');

      const size_t base_idx = dbl_v.size();
      dbl_v.resize(base_idx + nrow);
    
      constexpr size_t buf_size = max_chars_needed<T>();
      
      auto* __restrict dst = std::assume_aligned<64>(dbl_v.data() + base_idx);
      auto* __restrict src = std::assume_aligned<64>(x.data());

      std::memcpy(dst, src, nrow * sizeof(T));  
      
      for (auto& s : val_tmp)
          s.reserve(buf_size);                  
      
      constexpr size_t BATCH = 256;
      for (size_t i = 0; i < nrow; i += BATCH) {
          const size_t end = std::min(static_cast<size_t>(nrow), i + BATCH);
          alignas(64) char bufs[BATCH][buf_size];  
      
          #pragma GCC unroll 8
          for (size_t k = 0; k < end - i; ++k) {
              auto [ptr, ec] = std::to_chars(bufs[k], bufs[k] + buf_size, dst[i + k]);
              if (ec != std::errc{}) [[unlikely]] std::terminate();
              val_tmp[i + k].assign(bufs[k], ptr);
          }
      }

    } else if constexpr (std::is_same_v<T, char>) {

      matr_idx[1].push_back(ncol);
      type_refv.push_back('c');

      const size_t base_idx = chr_v.size();
      chr_v.resize(base_idx + nrow);
      constexpr size_t buf_size = 1;

      auto* __restrict dst = std::assume_aligned<64>(chr_v.data() + base_idx);
      auto* __restrict src = std::assume_aligned<64>(x.data());

      std::memcpy(dst, src, nrow * sizeof(char));  
      
      for (auto& s : val_tmp)
          s.reserve(buf_size);                  
      
      constexpr size_t BATCH = 1024;
      for (size_t i = 0; i < nrow; i += BATCH) {
          const size_t end = std::min(static_cast<size_t>(nrow), i + BATCH);
      
          #pragma GCC unroll 8
          for (size_t k = 0; k < end - i; ++k) {
              val_tmp[i + k].assign(1, src[i + k]);
          }
      }

    } else if constexpr (std::is_same_v<T, std::string>) {

      matr_idx[0].push_back(ncol);
      type_refv.push_back('s');
     
      const size_t base_idx = str_v.size();
      str_v.resize(base_idx + nrow);
      
      constexpr size_t BATCH = 256;
      for (size_t i = 0; i < nrow; i += BATCH) {
          const size_t end = std::min(static_cast<size_t>(nrow), i + BATCH);
      
          #pragma GCC unroll 8
          for (size_t k = i; k < end; ++k)
              str_v[base_idx + k] = x[k];
      
          #pragma GCC unroll 8
          for (size_t k = i; k < end; ++k)
              val_tmp[k] = x[k];
      }
      
    } else {
      std::cerr << "Error in (add_col) type not suported \n";
    };

    ncol += 1;
};



