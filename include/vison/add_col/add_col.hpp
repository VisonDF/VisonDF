#pragma once

template <typename T, bool Large = false> 
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

      if constexpr (Large) {

          for (i = 0; i < nrow; i += 1) {
              bool_v[base_idx + i] = x[i];
          };

          for (i = 0; i < nrow; i += 1) {
              char buf[buf_size];
              auto [ptr, ec] = std::to_chars(buf, buf + buf_size, 
                                             static_cast<int>(x[i]));

              if (ec == std::errc{}) [[likely]] {
                  val_tmp[i].assign(buf, ptr);
              } else [[unlikely]] {
                  std::terminate();
              }
          }

      } else if constexpr (!Large) {

          for (i = 0; i < nrow; i += 1) {

              const auto& val = x[i];

              bool_v[base_idx + i] = val;

              char buf[buf_size];
              auto [ptr, ec] = std::to_chars(buf, buf + buf_size, 
                                             static_cast<int>(val));

              if (ec == std::errc{}) [[likely]] {
                  val_tmp[i].assign(buf, ptr);
              } else [[unlikely]] {
                  std::terminate();
              }
          }

      }

    } else if constexpr (std::is_same_v<T, IntT>) {

      matr_idx[3].push_back(ncol);
      type_refv.push_back('i');

      const size_t base_idx = int_v.size();
      int_v.resize(base_idx + nrow);
    
      constexpr size_t buf_size = max_chars_needed<T>();
      size_t i;
      auto* __restrict dst = std::assume_aligned<64>(int_v.data() + base_idx);
      auto* __restrict src = std::assume_aligned<64>(x.data());

      for (auto& el : val_tmp) {
        el.reserve(buf_size);
      }

      if constexpr (Large) {

          #pragma GCC ivdep
          for (i = 0; i < nrow; i += 1) {
              dst[i] = src[i];
          };

          for (i = 0; i < nrow; i += 1) {
              char buf[buf_size];
              auto [ptr, ec] = std::to_chars(buf, buf + buf_size, x[i]);

              if (ec == std::errc{}) [[likely]] {
                  val_tmp[i].assign(buf, ptr);
              } else [[unlikely]] {
                  std::terminate();
              }
          }

      } else if constexpr (!Large) {

          for (i = 0; i < nrow; i += 1) {

              const auto& val = src[i];

              dst[i] = val;

              char buf[buf_size];
              auto [ptr, ec] = std::to_chars(buf, buf + buf_size, val);

              if (ec == std::errc{}) [[likely]] {
                  val_tmp[i].assign(buf, ptr);
              } else [[unlikely]] {
                  std::terminate();
              }
          }

      }

    } else if constexpr (std::is_same_v<T, UIntT>) {

      matr_idx[4].push_back(ncol);
      type_refv.push_back('u');
 
      const size_t base_idx = uint_v.size();
      uint_v.resize(base_idx + nrow);
    
      constexpr size_t buf_size = max_chars_needed<T>();
      size_t i;
      auto* __restrict dst = std::assume_aligned<64>(uint_v.data() + base_idx);
      auto* __restrict src = std::assume_aligned<64>(x.data());

      for (auto& el : val_tmp) {
        el.reserve(buf_size);
      }

      if constexpr (Large) {

          #pragma GCC ivdep
          for (i = 0; i < nrow; i += 1) {
              dst[i] = src[i];
          };

          for (i = 0; i < nrow; i += 1) {
              char buf[buf_size];
              auto [ptr, ec] = std::to_chars(buf, buf + buf_size, x[i]);

              if (ec == std::errc{}) [[likely]] {
                  val_tmp[i].assign(buf, ptr);
              } else [[unlikely]] {
                  std::terminate();
              }
          }

      } else if constexpr (!Large) {

          for (i = 0; i < nrow; i += 1) {

              const auto& val = src[i];

              dst[i] = val;

              char buf[buf_size];
              auto [ptr, ec] = std::to_chars(buf, buf + buf_size, val);

              if (ec == std::errc{}) [[likely]] {
                  val_tmp[i].assign(buf, ptr);
              } else [[unlikely]] {
                  std::terminate();
              }
          }

      }
      
    } else if constexpr (std::is_same_v<T, FloatT>) {

      matr_idx[5].push_back(ncol);
      type_refv.push_back('d');

      const size_t base_idx = dbl_v.size();
      dbl_v.resize(base_idx + nrow);
    
      constexpr size_t buf_size = max_chars_needed<T>();
      size_t i;
      auto* __restrict dst = std::assume_aligned<64>(dbl_v.data() + base_idx);
      auto* __restrict src = std::assume_aligned<64>(x.data());

      for (auto& el : val_tmp) {
        el.reserve(buf_size);
      }

      if constexpr (Large) {

          #pragma GCC ivdep
          for (i = 0; i < nrow; i += 1) {
              dst[i] = src[i];
          };

          for (i = 0; i < nrow; i += 1) {
              char buf[buf_size];
              auto [ptr, ec] = std::to_chars(buf, buf + buf_size, x[i]);

              if (ec == std::errc{}) [[likely]] {
                  val_tmp[i].assign(buf, ptr);
              } else [[unlikely]] {
                  std::terminate();
              }
          }

      } else if constexpr (!Large) {

          for (i = 0; i < nrow; i += 1) {

              const auto& val = src[i];

              dst[i] = val;

              char buf[buf_size];
              auto [ptr, ec] = std::to_chars(buf, buf + buf_size, val);

              if (ec == std::errc{}) [[likely]] {
                  val_tmp[i].assign(buf, ptr);
              } else [[unlikely]] {
                  std::terminate();
              }
          }

      }

    } else if constexpr (std::is_same_v<T, char>) {

      matr_idx[1].push_back(ncol);
      type_refv.push_back('c');

      const size_t base_idx = chr_v.size();
      chr_v.resize(base_idx + nrow);
     
      if constexpr (Large) {

        size_t i;
        auto* __restrict dst = std::assume_aligned<64>(chr_v.data() + base_idx);
        auto* __restrict src = std::assume_aligned<64>(x.data());

        #pragma GCC ivdep
        for (i = 0; i < nrow; i += 1) {
          dst[i] = src[i];
        };

        #pragma GCC ivdep
        for (i = 0; i < nrow; i += 1) {
          val_tmp[i].assign(1, x[i]);
        }

      } else if constexpr (!Large) {
 
        for (size_t i = 0; i < nrow; i += 1) {
          const auto& val_tmp2 = x[i];
          chr_v[base_idx + i] = val_tmp2;
          val_tmp[i].assign(1, val_tmp2);
        }; 

      }

    } else if constexpr (std::is_same_v<T, std::string>) {

      matr_idx[0].push_back(ncol);
      type_refv.push_back('s');
     
      const size_t base_idx = str_v.size();
      str_v.resize(base_idx + nrow);
     
      if constexpr (Large) {

        size_t i;

        for (i = 0; i < nrow; i += 1) {
          str_v[base_idx + i] = x[i];
        };

        for (i = 0; i < nrow; i += 1) {
          val_tmp[i] = x[i];
        }

      } else if constexpr (!Large) {
 
        for (size_t i = 0; i < nrow; i += 1) {
          const auto& val_tmp2 = x[i];
          str_v[base_idx + i] = val_tmp2;
          val_tmp[i] = val_tmp2;
        }; 

      }

    } else {
      std::cerr << "Error in (add_col) type not suported \n";
    };

    ncol += 1;
};



