#pragma once

template <bool ASC = 1, 
          unsigned int CORES = 4,
          bool Simd = true,
          SortType S = SortType::Radix,
          typename ComparatorFactory = DefaultComparatorFactory>
void sort_by(unsigned int& n) {

      static_assert(is_supported_sort<S>::value, 
                      "Sorting Method Not Supported");

      //auto cmp = make_cmp.template operator()<ASC, UIntT>(col);

      std::vector<size_t> idx(nrow);
      std::iota(idx.begin(), idx.end(), 0);
    
      unsigned int which = 999;
      unsigned int col_id = 0;

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
              sort_string<ASC, CORES, Simd, S, ComparatorFactory>(idx, nrow, col_id);
              break;
          }
          case 'c':
          {
              sort_char<ASC, CORES, Simd, S, ComparatorFactory>(idx, nrow, col_id);
              break;
          }
          case 'b':
          {
              sort_bool<ASC, CORES, Simd, S, ComparatorFactory>(idx, nrow, col_id); 
              break;
          }
          case 'i':
          {
              sort_integers<ASC, CORES, Simd, S, ComparatorFactory>(idx, nrow, col_id);
              break;
          }
          case 'u':
          {
              sort_uintegers<ASC, CORES, Simd, S, ComparatorFactory>(idx, nrow, col_id);
              break;
          }
          case 'd':
          {
              sort_flt<ASC, CORES, Simd, S, ComparatorFactory>(idx, nrow, col_id);
              break;
          }
      }

      std::vector<std::string> str_v2(nrow);
      
      permute_block<std::string>(
          str_v,
          tmp_val_refv,
          str_v2,
          matr_idx[0],
          idx,
          nrow);

      permute_block<char>(
          chr_v,
          tmp_val_refv,
          str_v2,
          matr_idx[1],
          idx,
          nrow);
      
     permute_block_bool(
         bool_v,
         tmp_val_refv,
         str_v2,
         matr_idx[2],
         idx,
         nrow);
      
      permute_block<IntT>(
          int_v,
          tmp_val_refv,
          str_v2,
          matr_idx[3],
          idx,
          nrow);
      
      permute_block<UIntT>(
          uint_v,
          tmp_val_refv,
          str_v2,
          matr_idx[4],
          idx,
          nrow);
      
      permute_block<FloatT>(
          dbl_v,
          tmp_val_refv,
          str_v2,
          matr_idx[5],
          idx,
          nrow);

      
};


