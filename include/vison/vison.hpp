#pragma once

// Standard
#include <algorithm>
#include <chrono>
#include <cmath>
#include <deque>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <numeric>
#include <regex>
#include <span>
#include <stdexcept>
#include <thread>
#include <variant>
#include <vector>

// System
#include <immintrin.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>

// Third-party
#include "external/ankerl/unordered_dense.h"
#include "external/fast_float/fast_float.h"
#include <omp.h>

namespace vison {

    #include "inlines/parse_rows_range_cached.inl"

    #include "inlines/parse_rows_range.inl"
    
    #include "inlines/simd_count_newlines.inl"
       
    #include "inlines/simd_can_be_nb.inl"

    #include "inlines/has_dot.inl"
    
    #include "types/PairHash.hpp"    
  
    #include "types/inference_type.hpp"

    template <typename Types = DefaultTypes>
    class Dataframe{
      private:

        using IntT = typename Types::IntT;
        using UIntT = typename Types::UIntT;
        using FloatT = typename Types::FloatT;
   
        #include "inlines/classify_column.inl"

        unsigned int nrow = 0;
        unsigned int ncol = 0;
      
        std::vector<std::string> str_v = {};
        std::vector<char> chr_v = {};
        std::vector<bool> bool_v = {};
        std::vector<IntT> int_v = {};
        std::vector<UIntT> uint_v = {};
        std::vector<FloatT> dbl_v = {};
       
        std::vector<std::vector<unsigned int>> matr_idx = {{}, {}, {}, {}, {}, {}};
        std::vector<std::string> name_v = {};
        std::vector<std::string> name_v_row = {};
        std::vector<unsigned int> longest_v = {};
    
        std::vector<char> type_refv = {};
        std::vector<std::vector<std::string>> tmp_val_refv = {};
    
        const std::vector<std::string>& get_str_vec() const {
          return str_v;
        };
    
        const std::vector<char>& get_chr_vec() const {
          return chr_v;
        };
    
        const std::vector<bool>& get_bool_vec() const {
          return bool_v;
        };
    
        const std::vector<IntT>& get_int_vec() const {
          return int_v;
        };
    
        const std::vector<UIntT>& get_uint_vec() const {
          return uint_v;
        };
    
        const std::vector<FloatT>& get_dbl_vec() const {
          return dbl_v;
        };
    
        const std::vector<std::vector<unsigned int>>& get_matr_idx() const {
          return matr_idx;
        };

        template <typename T>
        constexpr size_t max_chars_needed() noexcept {
            return std::numeric_limits<T>::digits10 + 3;
        }

        template <typename VecT>
        inline void append_block(std::vector<VecT>& dst,
                                 const std::vector<VecT>& src,
                                 size_t col_idx,
                                 size_t nrow)
        {

            const size_t start = col_idx * nrow;
            
            if constexpr (std::is_trivially_copyable_v<VecT>) {
                const size_t old_size = dst.size();
                dst.resize(old_size + nrow);
                std::memcpy(dst.data() + old_size,
                            src.data() + start,
                            nrow * sizeof(VecT));
            } else {
                dst.insert(dst.end(),
                           src.begin() + start,
                           src.begin() + start + nrow);
            }

        }

        #include "detail/longest_determine.hpp"
    
      public:
       
        #include "read_csv/readf.hpp"
        
        #include "read_csv/type_inference/type_classification.hpp"
    
        #include "display/display_filter.hpp"
         
        #include "display/display_filter_idx.hpp"
       
        #include "display/display.hpp"

        void reinitiate() {
         
          nrow = 0;
          ncol = 0;
     
          str_v = {};
          chr_v = {};
          bool_v = {};
          int_v = {};
          uint_v = {};
          dbl_v = {};
       
          matr_idx = {{}, {}, {}, {}, {}, {}};
          name_v = {};
          name_v_row = {};
          longest_v = {};
    
          type_refv = {};
          tmp_val_refv = {};
    
        };
 
        #include "fapply/fapply.hpp"
   
        #include "fapply/fapply_filter.hpp"

        #include "fapply/fapply_filter_idx.hpp"

        #include "fapply/vectorized_hint/fapply_simd_hint.hpp"

        #include "view_col/view_col.hpp"
        
        #include "view_col/view_colstr.hpp"
    
        #include "view_col/view_colchr.hpp"

        #include "view_col/view_colint.hpp"
         
        #include "view_col/view_coluint.hpp"
       
        #include "view_col/view_colflt.hpp"
       
        #include "get_col/get_col_filter.hpp"

        //#include "get_col/get_col_filter_idx.hpp"

        #include "get_col/get_col.hpp" 
        
        const std::vector<std::vector<std::string>>& get_tmp_val_refv() const {
          return tmp_val_refv;
        };
    
        const unsigned int& get_nrow() const {
          return nrow;
        };
    
        const unsigned int& get_ncol() const {
          return ncol;
        };
   
        #include "get_dataframe/get_dataframe.hpp"
    
        #include "get_dataframe/get_dataframe_filter.hpp"
         
        #include "get_dataframe/get_dataframe_filter_idx.hpp"
  
        #include "write_csv/writef.hpp"
   
        #include "replace_col/replace_col.hpp"
        
        #include "add_col/add_col.hpp"
        
        #include "rm_col/rm_col.hpp"
    
        #include "rm_row/rm_row.hpp"
   
        #include "transform/transform_inner.hpp"
         
        #include "transform/clean_memory/transform_inner_clean.hpp"
   
        #include "transform/transform_excluding.hpp"
         
        #include "transform/clean_memory/transform_excluding_clean.hpp"
      
        #include "merge/merge_inner.hpp"

        #include "transform/merge/no_dupplicates/transform_merge_inner2.hpp"
        
        #include "transform/merge/no_dupplicates/clean_memory/transform_merge_inner2_clean.hpp"

        #include "merge/no_dupplicates/merge_inner2.hpp"

        #include "merge/merge_excluding.hpp"

        #include "transform/merge/transform_merge_excluding.hpp"
   
        #include "transform/merge/clean_memory/transform_merge_excluding_clean.hpp"
   
        #include "merge/merge_excluding_both.hpp"

        #include "merge/merge_all.hpp"
        
        #include "merge/no_dupplicates/merge_all2.hpp"

        #include "transform/transform_left_join.hpp"
   
        #include "transform/transform_filter.hpp"

        #include "transform/transform_unique.hpp"

        #include "transform/clean_memory/transform_unique_clean.hpp"

        #include "transform/transform_group_by.hpp"
        
        #include "pivots/pivot_int.hpp"
    
        #include "pivots/pivot_uint.hpp"

        #include "pivots/pivot_dbl.hpp"

        #include "transform/sort_by.hpp"

        #include "concat/concat.hpp"
        
        void set_colname(std::vector<std::string> &x) {
          if (x.size() != ncol) {
            std::cout << "the number of columns of the dataframe does not correspond to the size of the input column name vector";
            return;
          } else {
            name_v = x;
          };
        };
    
        void set_rowname(std::vector<std::string> &x) {
          if (x.size() != nrow) {
            std::cout << "the number of columns of the dataframe does not correspond to the size of the input column name vector";
            return;
          } else {
            name_v_row = x;
          };
        };
    
        const std::vector<std::string>& get_colname() const {
          return name_v;
        };
    
        const std::vector<std::string>& get_rowname() const {
          return name_v_row;
        };
    
        const std::vector<char>& get_typecol() const {
          return type_refv;
        };
    
        Dataframe() {};
    
        ~Dataframe() {};
    
    };

}


