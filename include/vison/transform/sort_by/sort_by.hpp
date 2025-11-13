#pragma once

template <bool ASC = 1>
void sort_by(unsigned int& n) {

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
              auto values = std::span<const std::string>(str_v.data() + col_id * nrow, nrow);
              sort_idx_using_span_string<ASC>(idx, values);
              break;
          }
          case 'c':
          {
              auto values = std::span<const char>(chr_v.data() + col_id * nrow, nrow);
              sort_idx_using_span<ASC>(idx, values);
              break;
          }
          case 'b':
          {
              size_t base = col_id * nrow;
              sort_idx_bool<ASC>(idx, bool_v, base);
              break;
          }
          case 'i':
          {
              auto values = std::span<const IntT>(int_v.data() + col_id * nrow, nrow);
              sort_idx_using_span_integers<ASC>(idx, nrow, col_id);
              break;
          }
          case 'u':
          {
              auto values = std::span<const UIntT>(uint_v.data() + col_id * nrow, nrow);
              sort_idx_using_span_uintegers<ASC>(idx, nrow, col_id);
              break;
          }
          case 'd':
          {
              auto values = std::span<const FloatT>(dbl_v.data() + col_id * nrow, nrow);
              sort_idx_using_span<ASC>(idx, values);
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


