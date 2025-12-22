#pragma once

template <typename T,
          bool ASC = true,
          unsigned int CORES = 4,
          bool Simd = true,
          bool Soft = true,
          SortType S = SortType::Radix,
          bool BoolAsU8 = true,
          bool IsBoolCompressed = false>
void sort_by_external_mr(const std::vector<T>& nvec) {

     assert(("sort vec must be same size as dataframe nrow", 
                             nvec.size() == nrow));
     static_assert(is_supported_sort<S>::value, 
                     "Sorting Method Not Supported");

    if (!Soft && in_view) {
        std::cerr << "Can't use this operation while in `view` mode, " 
                  << "consider applying `.materialize()`\n";
        return;
    }

     std::vector<size_t> idx(nrow);
     std::iota(idx.begin(), idx.end(), 0);
    
     if constexpr (std::is_same_v<T, std::string>) {

         sort_string<ASC, CORES, Simd, S> (idx, nvec.data(), nrow);

     } else if constexpr (std::is_same_v<T, CharT>) {

         sort_char<ASC, CORES, Simd, S>   (idx, nvec.data(), nrow, df_charbuf_size);

     } else if constexpr (BoolAsU8) {

         sort_bool<ASC, 
                   CORES, 
                   Simd, 
                   S, 
                   BoolAsU8, 
                   IsBoolCompressed>(idx, nvec.data(), nrow);

     } else if constexpr(std::is_same_v<T, bool>) {

         sort_bool<ASC, 
                   CORES, 
                   Simd, 
                   S, 
                   BoolAsU8, 
                   IsBoolCompressed>(idx, nvec.data(), nrow);

     } else if constexpr (is_supported_int<T>::value) {

         sort_integers<ASC, CORES, Simd, S> (idx, nvec.data(), nrow);

     } else if constexpr (is_supported_uint<T>::value) {

         sort_uintegers<ASC, CORES, Simd, S>(idx, nvec.data(), nrow);

     } else if constexpr (is_supported_decimal<T>::value) {

         sort_flt<ASC, CORES, Simd, S>      (idx, nvec.data(), nrow);

     }

     if constexpr (!Soft)

         permute_block_mt<std::string, CORES>(
             str_v,
             tmp_val_refv,
             matr_idx[0],
             idx,
             nrow);

         permute_block_mt<CharT, CORES>(
             chr_v,
             tmp_val_refv,
             matr_idx[1],
             idx,
             nrow);
          
         permute_block_mt<uint8_t, CORES>(
             bool_v,
             tmp_val_refv,
             matr_idx[2],
             idx,
             nrow);
          
         permute_block_mt<IntT, CORES>(
             int_v,
             tmp_val_refv,
             matr_idx[3],
             idx,
             nrow);
         
         permute_block_mt<UIntT, CORES>(
             uint_v,
             tmp_val_refv,
             matr_idx[4],
             idx,
             nrow);
         
         permute_block_mt<FloatT, CORES>(
             dbl_v,
             tmp_val_refv,
             matr_idx[5],
             idx,
             nrow);

    } else {
         if (!in_view) {
             row_view_idx.resize(local_nrow);
             memcpy(row_view_idx.data(), 
                    idx.data(), 
                    local_nrow * sizeof(size_t));
             size_t i = 0;
             for (auto& el : idx) {
                 row_view_map.emplace(i, el);
                 i += 1;
             }
             in_view = true;
         } else {
             for (size_t i = 0; i < local_nrow; ++i) {
                 size_t current = i;
                 while (idx[current] != current) {
                     size_t next = idx[current];
                     std::swap(row_view_idx[current], row_view_idx[next]);
                     std::swap(idx[current], idx[next]);
                 }
             }
             for (size_t i = 0; i < local_norw; ++i)
                 row_view_map[i] = row_view_idx[i];
         }
    } 
};





