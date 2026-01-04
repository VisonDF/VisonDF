#pragma once

template <typename T, 
          bool Large = false, 
          bool BoolAsU8 = false> 
void add_col(const std::vector<T> &x, const std::string name = "NA") {
  
    if (x.size() != nrow) {
      std::cerr << "Error: vector length (" << x.size()
                << ") does not match nrow (" << nrow << ")\n";
      return;
    };
   
    const unsigned int local_nrow = nrow;

    name_v.push_back(name);

    auto copy_column = [](const size_t local_nrow,
                           auto* __restrict dst,
                           const auto* __restrict src) {
        if constexpr (Large) {
            #pragma GCC ivdep
            for (size_t i = 0; i < local_nrow; ++i) {
                dst[i] = src[i];
            }
            stringify_loop(nrow, val_tmp_data, src);
        } else {
            for (size_t i = 0; i < local_nrow; ++i) {
                dst[i] = src[i];
            }
        }
    };

    if constexpr (BoolAsU8) {

        matr_idx[2].push_back(ncol);
        type_refv.push_back('b');
        
        const size_t base_idx = bool_v.size();
        bool_v.resize(base_idx + 1);
        bool_v[base_idx].resize(local_nrow);

        auto* __restrict dst = std::assume_aligned<64>(bool_v[base_idx].data());
        auto* __restrict src = std::assume_aligned<64>(x.data());

        copy_column(local_nrow, dst, src);

    } else if constexpr (std::is_same_v<T, IntT>) {

        matr_idx[3].push_back(ncol);
        type_refv.push_back('i');

        const size_t base_idx = int_v.size();
        int_v.resize(base_idx + 1);
        int_v[base_idx].resize(local_nrow);
    
        auto* __restrict dst = std::assume_aligned<64>(int_v[base_idx].data());
        auto* __restrict src = std::assume_aligned<64>(x.data());

        copy_column(local_nrow, dst, src);

    } else if constexpr (std::is_same_v<T, UIntT>) {

        matr_idx[4].push_back(ncol);
        type_refv.push_back('u');
 
        const size_t base_idx = uint_v.size();
        uint_v.resize(base_idx + 1);
        uint_v[base_idx].resize(local_nrow);
    
        auto* __restrict dst = std::assume_aligned<64>(uint_v[base_idx].data());
        auto* __restrict src = std::assume_aligned<64>(x.data());

        copy_column(local_nrow, dst, src);

    } else if constexpr (std::is_same_v<T, FloatT>) {

        matr_idx[5].push_back(ncol);
        type_refv.push_back('d');

        const size_t base_idx = dbl_v.size();
        dbl_v.resize(base_idx + 1);
        dbl_v[base_idx].resize(local_nrow);
    
        auto* __restrict dst = std::assume_aligned<64>(dbl_v[base_idx].data());
        auto* __restrict src = std::assume_aligned<64>(x.data());

        copy_column(local_nrow, dst, src);

    } else if constexpr (std::is_same_v<T, CharT>) {

        matr_idx[1].push_back(ncol);
        type_refv.push_back('c');

        const size_t base_idx = chr_v.size();
        chr_v.resize(base_idx + 1);
        chr_v[base_idx].resize(local_nrow);
     
        auto* __restrict dst = std::assume_aligned<64>(chr_v[base_idx].data());
        auto* __restrict src = std::assume_aligned<64>(x.data());

        for (auto& s : val_tmp) {
            s.reserve(df_charbuf_size);
        }
        copy_column(local_nrow, dst, src);

    } else if constexpr (std::is_same_v<T, std::string>) {

        matr_idx[0].push_back(ncol);
        type_refv.push_back('s');
     
        const size_t base_idx = str_v.size();
        str_v.resize(base_idx + 1);
        str_v[base_idx].resize(local_nrow);

        auto* __restrict dst = std::assume_aligned<64>(str_v[base_idx].data());
        auto* __restrict src = std::assume_aligned<64>(x.data());

        copy_column(local_nrow, dst, src);

    } else {
      std::cerr << "Error in (add_col) type not suported \n";
    };

    ncol += 1;
};



