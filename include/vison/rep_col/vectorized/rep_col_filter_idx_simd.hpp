#pragma once

template <typename T> 
void rep_col_filter_idx_batch(std::vector<T> &x, 
                unsigned int &colnb,
                const std::vector<unsigned int>& mask) {

  if (x.size() != nrow) {
    std::cerr << "Error: vector length (" << x.size()
              << ") does not match nrow (" << nrow << ")\n";
    return;
  }
 
  unsigned int i;
  unsigned int i2 = 0;
  const unsigned int end_mask = mask.size();
  
  if constexpr (std::is_same_v<T, bool>) {

    while (i2 < matr_idx[2].size()) {
      if (colnb == matr_idx[2][i2]) {
        break;
      };
      i2 += 1;
    };

    if (i2 == matr_idx[2].size()) {
        std::cerr << "Error: column " << colnb << " not found for this type in (replace_col)\n";
        return;
    }

    i2 = nrow * i2;

    constexpr size_t BATCH = 8;
    constexpr size_t buf_size = 2;
    uint8_t lengths[BATCH];

    std::vector<std::string>& val_tmp = tmp_val_refv[colnb];
    for (auto& el : val_tmp) el.reserve(buf_size);
    
    alignas(64) char local_bufs[BATCH][buf_size];
    
    auto* __restrict dst = x.data();
    auto* __restrict src = bool_v.data();
    
    for (size_t i = 0; i < end_mask; i += BATCH) {
 
        const size_t end = std::min(i + BATCH, static_cast<size_t>(end_mask));
    
        #pragma GCC ivdep
        for (size_t j = i; j < end; ++j) {

            const size_t pos_idx = mask[j];

            auto& vl = src[pos_idx];
            auto& cur_buf = local_bufs[j - i];
            auto [ptr, ec] = std::to_chars(cur_buf, cur_buf + buf_size, vl);
            if (ec != std::errc{}) [[unlikely]] std::terminate();
            dst[pos_idx] = vl;
            lengths[j - i] = static_cast<size_t>(ptr - cur_buf);
        }
    
        for (size_t j = i; j < end; ++j) {

            const size_t pos_idx = mask[j];

            auto& cur_buf = local_bufs[j - i];
            const size_t len = lengths[j - i];
            val_tmp[pos_idx].resize(len);
            std::memcpy(val_tmp[pos_idx].data(), cur_buf, len);

        }
    }
    
  } else if constexpr (std::is_same_v<T, IntT>) {

    while (i2 < matr_idx[3].size()) {
      if (colnb == matr_idx[3][i2]) {
        break;
      };
      i2 += 1;
    };
    
    if (i2 == matr_idx[3].size()) {
        std::cerr << "Error: column " << colnb << " not found for this type in (replace_col)\n";
        return;
    }

    i2 = nrow * i2;

    constexpr size_t BATCH = 8;
    constexpr size_t buf_size = max_chars_needed<T>();
    uint8_t lengths[BATCH];

    std::vector<std::string>& val_tmp = tmp_val_refv[colnb];
    for (auto& el : val_tmp) el.reserve(buf_size);
    
    alignas(64) char local_bufs[BATCH][buf_size];
    
    auto* __restrict dst = x.data();
    auto* __restrict src = int_v.data();
    
    for (size_t i = 0; i < end_mask; i += BATCH) {
        const size_t end = std::min(i + BATCH, static_cast<size_t>(end_mask));
    
        #pragma GCC ivdep
        for (size_t j = i; j < end; ++j) {

            const size_t pos_idx = mask[j];

            auto& vl = src[pos_idx];
            auto& cur_buf = local_bufs[j - i];
            auto [ptr, ec] = std::to_chars(cur_buf, cur_buf + buf_size, vl);
            if (ec != std::errc{}) [[unlikely]] std::terminate();
            dst[pos_idx] = vl;
            lengths[j - i] = static_cast<size_t>(ptr - cur_buf);

        }
    
        for (size_t j = i; j < end; ++j) {
            
            const size_t pos_idx = mask[j];

            auto& cur_buf = local_bufs[j - i];
            const size_t len = lengths[j - i];
            val_tmp[pos_idx].resize(len);
            std::memcpy(val_tmp[pos_idx].data(), cur_buf, len);
        }
    }

  } else if constexpr (std::is_same_v<T, UIntT>) {

    while (i2 < matr_idx[4].size()) {
      if (colnb == matr_idx[4][i2]) {
        break;
      };
      i2 += 1;
    };
    
    if (i2 == matr_idx[4].size()) {
        std::cerr << "Error: column " << colnb << " not found for this type in (replace_col)\n";
        return;
    }

    i2 = nrow * i2;

    constexpr size_t BATCH = 8;
    constexpr size_t buf_size = max_chars_needed<T>();
    uint8_t lengths[BATCH];

    std::vector<std::string>& val_tmp = tmp_val_refv[colnb];
    for (auto& el : val_tmp) el.reserve(buf_size);
    
    alignas(64) char local_bufs[BATCH][buf_size];
    
    auto* __restrict dst = x.data();
    auto* __restrict src = uint_v.data();
    
    for (size_t i = 0; i < end_mask; i += BATCH) {
        const size_t end = std::min(i + BATCH, static_cast<size_t>(end_mask));
    
        #pragma GCC ivdep
        for (size_t j = i; j < end; ++j) {

            const size_t pos_idx = mask[j];

            auto& vl = src[pos_idx];
            auto& cur_buf = local_bufs[j - i];
            auto [ptr, ec] = std::to_chars(cur_buf, cur_buf + buf_size, vl);
            if (ec != std::errc{}) [[unlikely]] std::terminate();
            dst[pos_idx] = vl;
            lengths[j - i] = static_cast<size_t>(ptr - cur_buf);
        }
    
        for (size_t j = i; j < end; ++j) {

            const size_t pos_idx = mask[j];

            auto& cur_buf = local_bufs[j - i];
            const size_t len = lengths[j - i];
            val_tmp[pos_idx].resize(len);
            std::memcpy(val_tmp[pos_idx].data(), cur_buf, len);

        }
    }

  } else if constexpr (std::is_same_v<T, FloatT>) {

    while (i2 < matr_idx[5].size()) {
      if (colnb == matr_idx[5][i2]) {
        break;
      };
      i2 += 1;
    };
    
    if (i2 == matr_idx[5].size()) {
        std::cerr << "Error: column " << colnb << " not found for this type in (replace_col)\n";
        return;
    }

    i2 = nrow * i2;
    
    constexpr size_t BATCH = 8;
    constexpr size_t buf_size = max_chars_needed<T>();
    uint8_t lengths[BATCH];

    std::vector<std::string>& val_tmp = tmp_val_refv[colnb];
    for (auto& el : val_tmp) el.reserve(buf_size);
    
    alignas(64) char local_bufs[BATCH][buf_size];
    
    auto* __restrict dst = x.data();
    auto* __restrict src = str_v.data();
    
    for (size_t i = 0; i < end_mask; i += BATCH) {
        const size_t end = std::min(i + BATCH, static_cast<size_t>(end_mask));
    
        #pragma GCC ivdep
        for (size_t j = i; j < end; ++j) {

            const size_t pos_idx = mask[j];

            auto& vl = src[pos_idx];
            auto& cur_buf = local_bufs[j - i];
            auto [ptr, ec] = std::to_chars(cur_buf, cur_buf + buf_size, vl);
            if (ec != std::errc{}) [[unlikely]] std::terminate();
            dst[pos_idx] = vl;
            lengths[j - i] = static_cast<size_t>(ptr - cur_buf);
        }
    
        for (size_t j = i; j < end; ++j) {

            const size_t pos_idx = mask[j];

            auto& cur_buf = local_bufs[j - i];
            const size_t len = lengths[j - i];
            val_tmp[pos_idx].resize(len);
            std::memcpy(val_tmp[pos_idx].data(), cur_buf, len);
        }
    }

  } else if constexpr (std::is_same_v<T, std::string>) {

    while (i2 < matr_idx[0].size()) {
      if (colnb == matr_idx[0][i2]) {
        break;
      };
      i2 += 1;
    };
   
    if (i2 == matr_idx[0].size()) {
        std::cerr << "Error: column " << colnb << " not found for std::string in (replace_col)\n";
        return;
    }

    i2 = nrow * i2;

    auto* __restrict dst = str_v.data() + i2;
    const auto* __restrict src = x.data();
    
    #pragma unroll 32
    for (size_t i = 0; i < end_mask; ++i) {
      dst[mask[i]] = src[mask[i]];
    };

    std::vector<std::string>& __restrict val_tmp = tmp_val_refv[colnb];

    constexpr size_t BATCH = 32;
    alignas(64) std::string buf[BATCH];

    for (size_t i = 0; i < end_mask; i += BATCH) {
        const size_t end = std::min(i + BATCH, static_cast<size_t>(end_mask));
   
        for (size_t j = i; j < end; ++j) {

            const size_t pos_idx = mask[j];

            const std::string& str_vl = src[pos_idx];
            buf[j - i].assign(str_vl);
            
        }

        #pragma unroll 32 
        for (size_t j = i; j < end; ++j) {

            const size_t pos_idx = mask[j];

            val_tmp[pos_idx].assign(buf[j - i]);

        }
    }

  } else if constexpr (std::is_same_v<T, char>) {

    while (i2 < matr_idx[1].size()) {
      if (colnb == matr_idx[1][i2]) {
        break;
      };
      i2 += 1;
    }; 

    if (i2 == matr_idx[1].size()) {
        std::cerr << "Error: column " << colnb << " not found for this type in (replace_col)\n";
        return;
    }

    i2 = nrow * i2;

    auto* __restrict dst = chr_v.data() + i2;
    const auto* __restrict src = x.data();
    
    #pragma unroll 32
    for (size_t i = 0; i < end_mask; ++i) {
      dst[mask[i]] = src[mask[i]];
    };

    std::vector<std::string>& __restrict val_tmp = tmp_val_refv[colnb];

    constexpr size_t BATCH = 32;
    alignas(64) char buf[BATCH];

    for (size_t i = 0; i < end_mask; i += BATCH) {
        const size_t end = std::min(i + BATCH, static_cast<size_t>(end_mask));
    
        for (size_t j = i; j < end; ++j) {
            
            const size_t pos_idx = mask[j];

            buf[j - i] = src[pos_idx];

        }

        #pragma unroll 32 
        for (size_t j = i; j < end; ++j) {

            const size_t pos_idx = mask[j];

            val_tmp[pos_idx].assign(1, static_cast<char>(buf[j - i]));

        }
    }

  } else {
    std::cerr << "Error unsupported type in (replace_col)\n";
  };
};




