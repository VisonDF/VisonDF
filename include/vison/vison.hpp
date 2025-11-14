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

#if __has_include(<simd>)
  #include <simd>
  namespace v2 = std;  
#elif __has_include(<experimental/simd>)
  #include <experimental/simd>
  namespace v2 = std::experimental::parallelism_v2;
#else
  #error "No SIMD header found (need GCC 12+, Clang 15+, or MSVC 19.38+)"
#endif

// System
#include <immintrin.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>

// Third-party
#include "external/ankerl/unordered_dense.h"
#include "external/fast_float/fast_float.h"

#ifdef USE_DRAGONBOX
#include "external/dragonbox/dragonbox_to_chars.h"
#endif

#include <omp.h>

namespace vison {

    #ifdef USE_DRAGONBOX
    #include "inlines/fast_to_chars.inl"
    #else
    #include "inlines/fast_to_chars_no_dragonbox.inl"
    #endif

    #include "custom_simd_hash/simd_hash.hpp"

    #include "inlines/read_csv/parse_rows_range_cached.inl"

    #include "inlines/read_csv/parse_rows_range.inl"
    
    #include "inlines/read_csv/simd_count_newlines.inl"
       
    #include "inlines/simd_can_be_nb.inl"

    #include "inlines/get_dataframe/append_block.inl"
 
    #include "inlines/has_dot.inl"
    
    #include "types/PairHash.hpp"    
  
    #include "types/inference_type.hpp"

    #include "inlines/transform_aligned/match_group.inl"


    #include "inlines/radix_sorts/details/constants.inl"

    #include "inlines/radix_sorts/details/u8/get_local_histogram_16x.inl"
    #include "inlines/radix_sorts/details/u8/get_local_histogram_8x.inl" 
    #include "inlines/radix_sorts/details/u8/histogram_pass_u8_avx2.inl"
    #include "inlines/radix_sorts/details/u8/histogram_pass_u8_avx2_8x.inl"
    #include "inlines/radix_sorts/details/u8/histogram_pass_u8_avx512_16x.inl"
    #include "inlines/radix_sorts/details/u8/scatter_pass_u8_avx512.inl"

    #include "inlines/radix_sorts/details/u16/get_local_histogram_16x.inl"
    #include "inlines/radix_sorts/details/u16/get_local_histogram_8x.inl" 
    #include "inlines/radix_sorts/details/u16/histogram_pass_u16_avx2.inl"
    #include "inlines/radix_sorts/details/u16/histogram_pass_u16_avx2_8x.inl"
    #include "inlines/radix_sorts/details/u16/histogram_pass_u16_avx512_16x.inl"

    #include "inlines/radix_sorts/details/u32/get_local_histogram_16x.inl"
    #include "inlines/radix_sorts/details/u32/get_local_histogram_8x.inl" 
    #include "inlines/radix_sorts/details/u32/histogram_pass_u32_avx2_8buckets.inl"
    #include "inlines/radix_sorts/details/u32/histogram_pass_u32_avx2.inl"
    #include "inlines/radix_sorts/details/u32/histogram_pass_u32_avx512_16buckets.inl"
    #include "inlines/radix_sorts/details/u32/scatter_pass_u32_avx512.inl"

    #include "inlines/radix_sorts/radix_sort_uint8.inl"
    #include "inlines/radix_sorts/radix_sort_uint8_mt.inl"
    #include "inlines/radix_sorts/radix_sort_int32.inl"
    #include "inlines/radix_sorts/radix_sort_int32_mt.inl"
    #include "inlines/radix_sorts/radix_sort_uint32.inl"
    #include "inlines/radix_sorts/radix_sort_uint32.inl"
    //#include "inlines/radix_sorts/radix_sort_int16.inl"
    //#include "inlines/radix_sorts/radix_sort_uint16.inl"
    //#include "inlines/radix_sorts/radix_sort_int64.inl"
    //#include "inlines/radix_sorts/radix_sort_uint64.inl"


    #include "inlines/transform/sort_by/permute_block_bool.inl"
    #include "inlines/transform/sort_by/permute_block.inl"
    #include "inlines/transform/sort_by/sort_idx_bool.inl"
    #include "inlines/transform/sort_by/sort_idx_using_span.inl"
    #include "inlines/transform/sort_by/sort_idx_using_span_string.inl"

    template <typename Types = DefaultTypes>
    class Dataframe{
      private:
 
        using IntT = typename Types::IntT;
        using UIntT = typename Types::UIntT;
        using FloatT = typename Types::FloatT;
    
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
    
        [[nodiscard]] inline const std::vector<std::string>& get_str_vec() const {
          return str_v;
        };
    
        [[nodiscard]] inline const std::vector<char>& get_chr_vec() const {
          return chr_v;
        };
    
        [[nodiscard]] inline const std::vector<bool>& get_bool_vec() const {
          return bool_v;
        };
    
        [[nodiscard]] inline const std::vector<IntT>& get_int_vec() const {
          return int_v;
        };
    
        [[nodiscard]] inline const std::vector<UIntT>& get_uint_vec() const {
          return uint_v;
        };
    
        [[nodiscard]] inline const std::vector<FloatT>& get_dbl_vec() const {
          return dbl_v;
        };
    
        [[nodiscard]] inline const std::vector<std::vector<unsigned int>>& get_matr_idx() const {
          return matr_idx;
        };
   
        #include "inlines/classify_column.inl"
     
        #include "detail/longest_determine.hpp"
    
        #include "inlines/fapply/max_chars_needed.inl"

        #include "inlines/fapply/apply_numeric.inl"

        #include "inlines/fapply/apply_numeric_filter_range.inl"

        #include "inlines/fapply/apply_numeric_filter.inl"

        #include "inlines/fapply/apply_numeric_filter_idx.inl"

        #include "inlines/fapply/vectorized_hint/apply_numeric_simd.inl"

        #include "inlines/fapply/vectorized_hint/apply_numeric_simd_filter.inl"

        #include "inlines/fapply/vectorized_hint/apply_numeric_simd_filter_idx.inl"

        #include "inlines/fapply/vectorized_hint/apply_numeric_simd_filter_range.inl"

        #include "inlines/write_csv/estimate_row_size.inl"

      public:
       
        #include "read_csv/readf.hpp"
        
        #include "read_csv/type_inference/type_classification.hpp"
    
        #include "display/display_filter.hpp"
         
        #include "display/display_filter_idx.hpp"
       
        #include "display/display.hpp"

        #include "reinitiate.hpp"
         
        #include "fapply/fapply.hpp"
   
        #include "fapply/fapply_filter.hpp"

        #include "fapply/fapply_filter_range.hpp"

        #include "fapply/fapply_filter_idx.hpp"

        #include "fapply/vectorized_hint/fapply_simd_hint.hpp"

        #include "fapply/vectorized_hint/fapply_simd_hint_filter.hpp"

        #include "fapply/vectorized_hint/fapply_simd_hint_filter_idx.hpp"

        #include "fapply/vectorized_hint/fapply_simd_hint_filter_range.hpp"

        #include "view_col/view_col.hpp"
        
        #include "view_col/view_colstr.hpp"
    
        #include "view_col/view_colchr.hpp"

        #include "view_col/view_colint.hpp"
         
        #include "view_col/view_coluint.hpp"
       
        #include "view_col/view_colflt.hpp"
       
        #include "get_col/get_col_filter.hpp"

        #include "get_col/vectorized/get_col_filter_simd.hpp"

        #include "get_col/get_col_filter_idx.hpp"

        #include "get_col/vectorized/get_col_filter_idx_simd.hpp"

        #include "get_col/get_col_filter_range.hpp"

        #include "get_col/vectorized/get_col_filter_range_simd.hpp"

        #include "get_col/vectorized/get_col.hpp" 
        
        [[nodiscard]] const std::vector<std::vector<std::string>>& get_tmp_val_refv() const {
          return tmp_val_refv;
        };
    
        [[nodiscard]] inline const unsigned int& get_nrow() const {
          return nrow;
        };
    
        [[nodiscard]] inline const unsigned int& get_ncol() const {
          return ncol;
        };
   
        #include "get_dataframe/get_dataframe.hpp"
    
        #include "get_dataframe/get_dataframe_filter.hpp"
         
        #include "get_dataframe/get_dataframe_filter_range.hpp"

        #include "get_dataframe/vectorized/get_dataframe_filter_simd.hpp"

        #include "get_dataframe/vectorized/get_dataframe_filter_range_simd.hpp"

        #include "get_dataframe/get_dataframe_filter_idx.hpp"
  
        #include "get_dataframe/vectorized/get_dataframe_filter_idx_simd.hpp"

        #include "write_csv/writef.hpp"
   
        #include "rep_col/rep_col.hpp"
        
        #include "rep_col/batched/rep_col_batch.hpp"

        #include "rep_col/rep_col_filter.hpp"

        #include "rep_col/batched/rep_col_filter_batch.hpp"

        #include "rep_col/rep_col_filter_range.hpp"

        #include "rep_col/rep_col_filter_idx.hpp"

        #include "rep_col/batched/rep_col_filter_idx_batch.hpp"

        #include "rep_col/batched/rep_col_filter_range_batch.hpp"

        #include "add_col/add_col.hpp"
        
        #include "add_col/batched/add_col_batch.hpp"

        #include "rm_col/rm_col.hpp"

        #include "rm_col/rm_col_range.hpp"
    
        #include "rm_col/rm_col_range_reconstruct.hpp"

        #include "rm_row/rm_row.hpp"
 
        #include "rm_row/batched/rm_row_batch.hpp"
  
        #include "rm_row/rm_row_range.hpp"
 
        #include "rm_row/batched/rm_row_range_batch.hpp"

        #include "rm_row/rm_row_range_reconstruct.hpp"

        #include "rm_row/vectorized/rm_row_range_reconstruct_simd.hpp"

        #include "transform/transform_inner/transform_inner.hpp"
        
        #ifdef _OPENMP
        #include "transform/transform_inner/transform_inner_mt.hpp"
        #endif

        #include "transform/transform_excluding/transform_excluding.hpp"
 
        #ifdef _OPENMP
        #include "transform/transform_excluding/transform_excluding_mt.hpp"
        #endif
        
        #include "merge/merge_inner.hpp"

        #include "transform/merge/no_dupplicates/transform_merge_inner2.hpp"
        
        #include "merge/no_dupplicates/merge_inner2.hpp"

        #include "merge/merge_excluding.hpp"

        #include "transform/merge/transform_merge_excluding.hpp"
   
        #include "merge/merge_excluding_both.hpp"

        #include "merge/merge_all.hpp"
        
        #include "merge/no_dupplicates/merge_all2.hpp"

        #include "transform/transform_left_join/transform_left_join.hpp"

        #ifdef _OPENMP
        #include "transform/transform_left_join/transform_left_join_simd.hpp"
        #include "transform/transform_left_join/transform_left_join_mt.hpp"
        #endif

        #include "transform/transform_left_join/transform_left_join_aligned.hpp"

        #ifdef _OPENMP
        #include "transform/transform_left_join/transform_left_join_aligned_simd.hpp"
        #include "transform/transform_left_join/transform_left_join_aligned_mt.hpp"
        #endif

        #include "one_to_many_join/otm.hpp"

        #ifdef _OPENMP
        #include "one_to_many_join/otm_simd.hpp"
        #include "one_to_many_join/otm_mt.hpp"
        #endif

        #include "transform/transform_filter/transform_filter.hpp"

        #ifdef _OPENMP
        #include "transform/transform_filter/transform_filter_mt.hpp"
        #endif

        #include "transform/transform_filter/transform_filter_range.hpp"
 
        #ifdef _OPENMP
        #include "transform/transform_filter/transform_filter_range_mt.hpp"
        #endif
 
        #include "transform/transform_filter/transform_filter_idx.hpp"

        #ifdef _OPENMP
        #include "transform/transform_filter/transform_filter_idx_mt.hpp"
        #endif

        #include "transform/transform_unique/transform_unique.hpp"

        #ifdef _OPENMP
        #include "transform/transform_unique/transform_unique_mt.hpp"
        #endif

        #include "transform/transform_group_by/transform_group_by.hpp"

        #ifdef _OPENMP
        #include "transform/transform_group_by/transform_group_by_mt.hpp"
        #endif
        
        #include "pivots/pivot_int.hpp"

        #ifdef _OPENMP
        #include "pivots/pivot_int_mt.hpp"
        #endif

        #include "pivots/pivot_uint.hpp"

        #ifdef _OPENMP
        #include "pivots/pivot_uint_mt.hpp"
        #endif

        #include "pivots/pivot_dbl.hpp"

        #ifdef _OPENMP
        #include "pivots/pivot_dbl_mt.hpp"
        #endif

        #include "transform/sort_by/sort_by.hpp"

        #include "concat/concat.hpp"
            
        #include "set_colname.hpp"

        #include "set_rowname.hpp"
            
        [[nodiscard]] inline const std::vector<std::string>& get_colname() const {
          return name_v;
        };
    
        [[nodiscard]] inline const std::vector<std::string>& get_rowname() const {
          return name_v_row;
        };
    
        [[nodiscard]] inline const std::vector<char>& get_typecol() const {
          return type_refv;
        };
    
        Dataframe() = default;
    
        ~Dataframe() = default;
    
    };

}


