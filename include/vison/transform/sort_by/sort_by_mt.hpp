#pragma once

template <bool ASC = 1, 
          unsigned int CORES = 4,
          typename T = void,
          bool Simd = true,
          bool Soft = true,
          SortType S = SortType::Radix,
          typename ComparatorFactory = DefaultComparatorFactory>
void sort_by_mt(unsigned int& n) {

    static_assert(is_supported_sort<S>::value, 
                    "Sorting Method Not Supported");

    if (!Soft && in_view) {
        std::cerr << "Can't use this operation while in `view` mode, " 
                  << "consider applying `.materialize()`\n";
        return;
    }

    const unsigned int local_nrow = nrow;
    std::vector<size_t> idx;
    idx.resize(local_nrow);
    std::iota(idx_storage.begin(), idx_storage.end(), 0);
  
    unsigned int which = 999;
    unsigned int col_id = 0;

    if constexpr (std::is_same_v<T, void>) {

        switch (type_refv[n])
        {
            case 's': which = 0; break;
            case 'c': which = 1; break;
            case 'b': which = 2; break;
            case 'i': which = 3; break;
            case 'u': which = 4; break;
            case 'd': which = 5; break;
            default:
                std::cerr << "Unknown type\n";
                return;
        }

    } else {

        if constexpr (std::is_same_v<element_type_t<T>, std::string>) {
            which = 0;
        } else if constexpr (std::is_same_v<element_type_t<T>, CharT>) {
            which = 1;
        } else if constexpr (std::is_same_v<element_type_t<T>, uint8_t>) {
            which = 2;
        } else if constexpr (std::is_same_v<element_type_t<T>, IntT>) {
            which = 3;
        } else if constexpr (std::is_same_v<element_type_t<T>, UIntT>) {
            which = 4;
        } else if constexpr (std::is_same_v<element_type_t<T>, FloatT>) {
            which = 5;
        }

    }

    auto& m = matr_idx[which];
    while (col_id < m.size() && n != m[col_id])
        ++col_id;

    if (col_id == m.size())
    {
        std::cerr << "Column not found\n";
        return;
    }

    switch (type_refv[n])
    {
        case 's':
        {
            const std::string* keys = str_v[col_id].data();
            sort_string<ASC, CORES, Simd, S, ComparatorFactory>(idx, 
                                                                keys, 
                                                                nrow);
            break;
        }
        case 'c':
        {
            const int8_t (*keys)[df_charbuf_size] = reinterpret_cast<const int8_t (*)[df_charbuf_size]>(chr_v[col_id].data());
            sort_char<ASC, CORES, Simd, S, ComparatorFactory>  (idx, 
                                                                keys,
                                                                nrow,
                                                                df_charbuf_size);
            break;
        }
        case 'b':
        {
            const uint8_t* keys = dbl_v[col_id].data();
            sort_bool<ASC, CORES, Simd, S, false, ComparatorFactory>(idx, keys, nrow); 
            break;
        }
        case 'i':
        {
            const IntT* keys = int_v[col_id].data();
            sort_integers<ASC, CORES, Simd, S, ComparatorFactory>   (idx, keys, nrow);
            break;
        }
        case 'u':
        {
            const UIntT* keys = uint_v[col_id].data();
            sort_uintegers<ASC, CORES, Simd, S, ComparatorFactory>  (idx, keys, nrow);
            break;
        }
        case 'd':
        {
            const FloatT* keys = dbl_v[col_id].data();
            sort_flt<ASC, CORES, Simd, S, ComparatorFactory>        (idx, keys, nrow);
            break;
        }
    }

    if constexpr (!Soft) {
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
         }
    }

};




