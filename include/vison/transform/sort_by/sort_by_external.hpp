#pragma once

template <bool ASC = 1,
          bool Simd = true,
          SortType S = SortType::Radix,
          bool BoolAsU8 = true,
          bool IsBoolCompressed = false,
          typename T>
void sort_by_external(const std::vector<T>& nvec) {

   assert(("sort vec must be same size as dataframe nrow", 
                           nvec.size() == nrow));
   static_assert(is_supported_sort<S>::value, 
                   "Sorting Method Not Supported");

   std::vector<size_t> idx(nrow);
   std::iota(idx.begin(), idx.end(), 0);
  
   if constexpr (std::is_same_v<T, std::string>) {

       sort_string<ASC, 1, Simd, S> (idx, nvec.data(), nrow);

   } else if constexpr (std::is_same_v<T, CharT>) {

       sort_char<ASC, 1, Simd, S>   (idx, nvec.data(), nrow, df_charbuf_size);

   } else if constexpr (BoolAsU8) {

           sort_bool<ASC, 
                     1, 
                     Simd, 
                     S, 
                     BoolAsU8, 
                     IsBoolCompressed>(idx, nvec.data(), nrow);

   } else if constexpr(std::is_same_v<T, bool>) {

           sort_bool<ASC, 
                     1, 
                     Simd, 
                     S, 
                     BoolAsU8, 
                     IsBoolCompressed>(idx, nvec.data(), nrow);

   } else if constexpr (is_supported_int<T>::value) {

           sort_integers<ASC, 1, Simd, S> (idx, nvec.data(), nrow);

   } else if constexpr (is_supported_uint<T>::value) {

           sort_uintegers<ASC, 1, Simd, S>(idx, nvec.data(), nrow);

   } else if constexpr (is_supported_decimal<T>::value) {

           sort_flt<ASC, 1, Simd, S>      (idx, nvec.data(), nrow);

   }

   permute_block<std::string>(
       str_v,
       tmp_val_refv,
       matr_idx[0],
       idx,
       nrow);

   permute_block<CharT>(
       chr_v,
       tmp_val_refv,
       matr_idx[1],
       idx,
       nrow);
    
   permute_block<uint8_t>(
       bool_v,
       tmp_val_refv,
       matr_idx[2],
       idx,
       nrow);
    
    permute_block<IntT>(
        int_v,
        tmp_val_refv,
        matr_idx[3],
        idx,
        nrow);
    
    permute_block<UIntT>(
        uint_v,
        tmp_val_refv,
        matr_idx[4],
        idx,
        nrow);
    
    permute_block<FloatT>(
        dbl_v,
        tmp_val_refv,
        matr_idx[5],
        idx,
        nrow);
      
};


