#pragma once
#include <iomanip>
#include <iostream>
#include <vector>
#include <unordered_set>
#include <unordered_map>
#include <deque>
#include <cmath>
#include <chrono>
#include <immintrin.h>
#include <cstddef>
#include <cstdint>
#include <thread>
#include <map>
#include <fstream>
#include <algorithm>
#include <stdexcept>
#include <numeric>
#include <regex>
#include "../../third_party/ankerl/unordered_dense.h"
#include <omp.h>
#include <sys/mman.h>
#include <sys/stat.h> 
#include <fcntl.h>
#include "../../third_party/fast_float/include/fast_float/fast_float.h"
#include <variant>
#include <span>

//@L1 The Dataframe Object

//@T Dataframe
//@U Dataframe my_dataframe
//@X
//@D Dataframe objects supporting writing / reading csv, storing columns in differents vectors type (automatically or not), specifying the rows and columns to copy or get by reference, perform all types of joins, groupby... See examples.
//@A See_below : See below
//@X
//@E See below
//@X

namespace vison {

    #include "inlines/parse_rows_range_cached.inl"

    #include "inlines/parse_rows_range.inl"
    
    #include "inlines/simd_count_newlines.inl"
       
    #include "inlines/simd_can_be_nb.inl"

    #include "inlines/has_dot.inl"
   
    #include "inlines/classify_column.inl"
 
    #include "types/PairHash.hpp"    
   
    class Dataframe{
      private:
        
        unsigned int nrow = 0;
        unsigned int ncol = 0;
      
        std::vector<std::string> str_v = {};
        std::vector<char> chr_v = {};
        std::vector<bool> bool_v = {};
        std::vector<int> int_v = {};
        std::vector<unsigned int> uint_v = {};
        std::vector<double> dbl_v = {};
       
        std::vector<int> pre_str_v = {};
        std::vector<unsigned int> pre_chr_v = {};
    
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
    
        const std::vector<int>& get_int_vec() const {
          return int_v;
        };
    
        const std::vector<unsigned int>& get_uint_vec() const {
          return uint_v;
        };
    
        const std::vector<double>& get_dbl_vec() const {
          return dbl_v;
        };
    
        const std::vector<std::vector<unsigned int>>& get_matr_idx() const {
          return matr_idx;
        };

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
       
          pre_str_v = {};
          pre_chr_v = {};
    
          matr_idx = {{}, {}, {}, {}, {}, {}};
          name_v = {};
          name_v_row = {};
          longest_v = {};
    
          type_refv = {};
          tmp_val_refv = {};
    
        };

        #include "fapply/fapply.hpp"
   
        #include "view_col/view_colnb.hpp"
        
        #include "view_col/view_colstr.hpp"
    
        #include "view_col/view_colchr.hpp"

        #include "view_col/view_colint.hpp"
         
        #include "view_col/view_coluint.hpp"
       
        #include "view_col/view_coldbl.hpp"
       
        #include "get_col/get_col_filter.hpp"

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
            
        void transform_inner_clean(Dataframe &cur_obj, unsigned int &in_col, unsigned int &ext_col) {
          unsigned int i2;
          const auto& cur_tmp = cur_obj.get_tmp_val_refv();
          const std::vector<std::string>& ext_colv = cur_tmp[ext_col];
          const std::vector<std::string>& in_colv = tmp_val_refv[in_col];
          std::string cur_val;
          unsigned int nrow2 = nrow;
          unsigned int pos_colv;
          nrow = 0;
          const unsigned int& ext_nrow = cur_obj.get_nrow();
    
          //std::unordered_set<std::string> lookup; // standard set (slower)
          ankerl::unordered_dense::set<std::string> lookup;
    
          lookup.reserve(ext_nrow);
          for (unsigned int j = 0; j < ext_nrow; ++j)
            lookup.insert(ext_colv[j]);
    
          for (int i = 0; i < nrow2; ++i) {
            if (lookup.contains(in_colv[i])) {
              for (i2 = 0; i2 < matr_idx[0].size(); i2 += 1) {
                pos_colv = matr_idx[0][i2];
                str_v[nrow2 * i2 + nrow] = str_v[nrow2 * i2 + i];
                tmp_val_refv[pos_colv][nrow] = tmp_val_refv[pos_colv][i];
              };
              for (i2 = 0; i2 < matr_idx[1].size(); i2 += 1) {
                pos_colv = matr_idx[1][i2];
                chr_v[nrow2 * i2 + nrow] = chr_v[nrow2 * i2 + i];
                tmp_val_refv[pos_colv][nrow] = tmp_val_refv[pos_colv][i];
              };
              for (i2 = 0; i2 < matr_idx[2].size(); i2 += 1) {
                pos_colv = matr_idx[2][i2];
                bool_v[nrow2 * i2 + nrow] = bool_v[nrow2 * i2 + i];
                tmp_val_refv[pos_colv][nrow] = tmp_val_refv[pos_colv][i];
              };
              for (i2 = 0; i2 < matr_idx[3].size(); i2 += 1) {
                pos_colv = matr_idx[3][i2];
                int_v[nrow2 * i2 + nrow] = int_v[nrow2 * i2 + i];
                tmp_val_refv[pos_colv][nrow] = tmp_val_refv[pos_colv][i];
              };
              for (i2 = 0; i2 < matr_idx[4].size(); i2 += 1) {
                pos_colv = matr_idx[4][i2];
                uint_v[nrow2 * i2 + nrow] = uint_v[nrow2 * i2 + i];
                tmp_val_refv[pos_colv][nrow] = tmp_val_refv[pos_colv][i];
              };
              for (i2 = 0; i2 < matr_idx[5].size(); i2 += 1) {
                pos_colv = matr_idx[5][i2];
                dbl_v[nrow2 * i2 + nrow] = dbl_v[nrow2 * i2 + i];
                tmp_val_refv[pos_colv][nrow] = tmp_val_refv[pos_colv][i];
              };
              nrow += 1;
            };
          };
          unsigned int delta_col = nrow2 - nrow;
          unsigned int pos_colv2;
          for (i2 = 0; i2 < matr_idx[0].size(); i2 += 1) {
            pos_colv = matr_idx[0][i2];
            pos_colv2 = (nrow + 1) * i2;
            str_v.erase(str_v.begin() + pos_colv2, 
                            str_v.begin() + pos_colv2 + delta_col);
            tmp_val_refv[pos_colv].erase(tmp_val_refv[pos_colv].begin() + nrow, 
                            tmp_val_refv[pos_colv].end());
            tmp_val_refv[pos_colv].shrink_to_fit();
          };
          str_v.shrink_to_fit();
          for (i2 = 0; i2 < matr_idx[1].size(); i2 += 1) {
            pos_colv = matr_idx[1][i2];
            pos_colv2 = (nrow + 1) * i2;
            chr_v.erase(chr_v.begin() + pos_colv2, 
                            chr_v.begin() + pos_colv2 + delta_col);
            tmp_val_refv[pos_colv].erase(tmp_val_refv[pos_colv].begin() + nrow, 
                            tmp_val_refv[pos_colv].end());
            tmp_val_refv[pos_colv].shrink_to_fit();
          };
          chr_v.shrink_to_fit();
          for (i2 = 0; i2 < matr_idx[2].size(); i2 += 1) {
            pos_colv = matr_idx[2][i2];
            pos_colv2 = (nrow + 1) * i2;
            bool_v.erase(bool_v.begin() + pos_colv2, 
                            bool_v.begin() + pos_colv2 + delta_col);
            tmp_val_refv[pos_colv].erase(tmp_val_refv[pos_colv].begin() + nrow, 
                            tmp_val_refv[pos_colv].end());
            tmp_val_refv[pos_colv].shrink_to_fit();
          };
          bool_v.shrink_to_fit();
          for (i2 = 0; i2 < matr_idx[3].size(); i2 += 1) {
            pos_colv = matr_idx[3][i2];
            pos_colv2 = (nrow + 1) * i2;
            int_v.erase(int_v.begin() + pos_colv2, 
                            int_v.begin() + pos_colv2 + delta_col);
            tmp_val_refv[pos_colv].erase(tmp_val_refv[pos_colv].begin() + nrow, 
                            tmp_val_refv[pos_colv].end());
            tmp_val_refv[pos_colv].shrink_to_fit();
          };
          int_v.shrink_to_fit();
          for (i2 = 0; i2 < matr_idx[4].size(); i2 += 1) {
            pos_colv = matr_idx[4][i2];
            pos_colv2 = (nrow + 1) * i2;
            uint_v.erase(uint_v.begin() + pos_colv2, 
                            uint_v.begin() + pos_colv2 + delta_col);
            tmp_val_refv[pos_colv].erase(tmp_val_refv[pos_colv].begin() + nrow, 
                            tmp_val_refv[pos_colv].end());
            tmp_val_refv[pos_colv].shrink_to_fit();
          };
          uint_v.shrink_to_fit();
          for (i2 = 0; i2 < matr_idx[5].size(); i2 += 1) {
            pos_colv = matr_idx[5][i2];
            pos_colv2 = (nrow + 1) * i2;
            dbl_v.erase(dbl_v.begin() + pos_colv2, 
                            dbl_v.begin() + pos_colv2 + delta_col);
            tmp_val_refv[pos_colv].erase(tmp_val_refv[pos_colv].begin() + nrow, 
                            tmp_val_refv[pos_colv].end());
            tmp_val_refv[pos_colv].shrink_to_fit();
          };
          dbl_v.shrink_to_fit();
        };
    
        void transform_excluding(Dataframe &cur_obj, unsigned int &in_col, unsigned int &ext_col) {
          unsigned int i2;
          unsigned int nrow2 = nrow;
          nrow = 0;
          std::vector<std::vector<std::string>> cur_tmp = cur_obj.get_tmp_val_refv();
          const std::vector<std::string>& ext_colv = cur_tmp[ext_col];
          const std::vector<std::string>& in_colv = tmp_val_refv[in_col];
          std::string cur_val;
          unsigned int pos_vl;
          const unsigned int& ext_nrow = cur_obj.get_nrow();
    
          //std::unordered_set<std::string> lookup; // standard set (slower)
          ankerl::unordered_dense::set<std::string> lookup; 
    
          lookup.reserve(ext_nrow);
          for (int j = 0; j < ext_nrow; j += 1) {
            lookup.insert(ext_colv[j]);
          };
    
          for (int i = 0; i < nrow2; i += 1) {
            if (!(lookup.contains(in_colv[i]))) {
              for (i2 = 0; i2 < matr_idx[0].size(); i2 += 1) {
                pos_vl = matr_idx[0][i2];
                str_v[i2 * nrow2 + nrow] = str_v[i2 * nrow2 + i];
                tmp_val_refv[pos_vl][nrow] = tmp_val_refv[pos_vl][i];
              };
              for (i2 = 0; i2 < matr_idx[1].size(); i2 += 1) {
                pos_vl = matr_idx[1][i2];
                chr_v[i2 * nrow2 + nrow] = chr_v[i2 * nrow2 + i];
                tmp_val_refv[pos_vl][nrow] = tmp_val_refv[pos_vl][i];
              };
              for (i2 = 0; i2 < matr_idx[2].size(); i2 += 1) {
                pos_vl = matr_idx[2][i2];
                bool_v[i2 * nrow2 + nrow] = bool_v[i2 * nrow2 + i];
                tmp_val_refv[pos_vl][nrow] = tmp_val_refv[pos_vl][i];
              };
              for (i2 = 0; i2 < matr_idx[3].size(); i2 += 1) {
                pos_vl = matr_idx[3][i2];
                int_v[i2 * nrow2 + nrow] = int_v[i2 * nrow2 + i];
                tmp_val_refv[pos_vl][nrow] = tmp_val_refv[pos_vl][i];
              };
              for (i2 = 0; i2 < matr_idx[4].size(); i2 += 1) {
                pos_vl = matr_idx[4][i2];
                uint_v[i2 * nrow2 + nrow] = uint_v[i2 * nrow2 + i];
                tmp_val_refv[pos_vl][nrow] = tmp_val_refv[pos_vl][i];
              };
              for (i2 = 0; i2 < matr_idx[5].size(); i2 += 1) {
                pos_vl = matr_idx[5][i2];
                dbl_v[i2 * nrow2 + nrow] = dbl_v[i2 * nrow2 + i];
                tmp_val_refv[pos_vl][nrow] = tmp_val_refv[pos_vl][i];
              };
    
              nrow += 1;
            };
          };
        };
    
        void transform_excluding_clean(Dataframe &cur_obj, unsigned int &in_col, unsigned int &ext_col) {
          unsigned int i2;
          unsigned int nrow2 = nrow;
          nrow = 0;
          std::vector<std::vector<std::string>> cur_tmp = cur_obj.get_tmp_val_refv();
          const std::vector<std::string>& ext_colv = cur_tmp[ext_col];
          const std::vector<std::string>& in_colv = tmp_val_refv[in_col];
          std::string cur_val;
          unsigned int pos_vl;
          const unsigned int& ext_nrow = cur_obj.get_nrow();
    
          //std::unordered_set<std::string> lookup; // standard set (slower)
          ankerl::unordered_dense::set<std::string> lookup;
    
          lookup.reserve(ext_nrow);
          for (auto &j : ext_colv) {
            lookup.insert(j);
          };
    
          for (int i = 0; i < nrow2; i += 1) {
            if (!(lookup.contains(in_colv[i]))) {
              for (i2 = 0; i2 < matr_idx[0].size(); i2 += 1) {
                pos_vl = matr_idx[0][i2];
                str_v[i2 * nrow2 + nrow] = str_v[i2 * nrow2 + i];
                tmp_val_refv[pos_vl][nrow] = tmp_val_refv[pos_vl][i];
              };
              for (i2 = 0; i2 < matr_idx[1].size(); i2 += 1) {
                pos_vl = matr_idx[1][i2];
                chr_v[i2 * nrow2 + nrow] = chr_v[i2 * nrow2 + i];
                tmp_val_refv[pos_vl][nrow] = tmp_val_refv[pos_vl][i];
              };
              for (i2 = 0; i2 < matr_idx[2].size(); i2 += 1) {
                pos_vl = matr_idx[2][i2];
                bool_v[i2 * nrow2 + nrow] = bool_v[i2 * nrow2 + i];
                tmp_val_refv[pos_vl][nrow] = tmp_val_refv[pos_vl][i];
              };
              for (i2 = 0; i2 < matr_idx[3].size(); i2 += 1) {
                pos_vl = matr_idx[3][i2];
                int_v[i2 * nrow2 + nrow] = int_v[i2 * nrow2 + i];
                tmp_val_refv[pos_vl][nrow] = tmp_val_refv[pos_vl][i];
              };
              for (i2 = 0; i2 < matr_idx[4].size(); i2 += 1) {
                pos_vl = matr_idx[4][i2];
                uint_v[i2 * nrow2 + nrow] = uint_v[i2 * nrow2 + i];
                tmp_val_refv[pos_vl][nrow] = tmp_val_refv[pos_vl][i];
              };
              for (i2 = 0; i2 < matr_idx[5].size(); i2 += 1) {
                pos_vl = matr_idx[5][i2];
                dbl_v[i2 * nrow2 + nrow] = dbl_v[i2 * nrow2 + i];
                tmp_val_refv[pos_vl][nrow] = tmp_val_refv[pos_vl][i];
              };
    
              nrow += 1;
            };
          };
    
          unsigned delta_col = nrow2 - nrow;
          unsigned int pos_vl2;
          for (i2 = 0; i2 < matr_idx[0].size(); i2 += 1) {
            pos_vl = matr_idx[0][i2];
            pos_vl2 = (nrow + 1) * i2;
            str_v.erase(str_v.begin() + pos_vl2, 
                            str_v.begin() + pos_vl2 + delta_col);
            tmp_val_refv[pos_vl].erase(tmp_val_refv[pos_vl].begin() + nrow, 
                            tmp_val_refv[pos_vl].end());
            tmp_val_refv[pos_vl].shrink_to_fit();
          };
          str_v.shrink_to_fit();
          for (i2 = 0; i2 < matr_idx[1].size(); i2 += 1) {
            pos_vl = matr_idx[1][i2];
            pos_vl2 = (nrow + 1) * i2;
            chr_v.erase(chr_v.begin() + pos_vl2, 
                            chr_v.begin() + pos_vl2 + delta_col);
            tmp_val_refv[pos_vl].erase(tmp_val_refv[pos_vl].begin() + nrow, 
                            tmp_val_refv[pos_vl].end());
            tmp_val_refv[pos_vl].shrink_to_fit();
          };
          chr_v.shrink_to_fit();
          for (i2 = 0; i2 < matr_idx[2].size(); i2 += 1) {
            pos_vl = matr_idx[2][i2];
            pos_vl2 = (nrow + 1) * i2;
            bool_v.erase(bool_v.begin() + pos_vl2, 
                            bool_v.begin() + pos_vl2 + delta_col);
            tmp_val_refv[pos_vl].erase(tmp_val_refv[pos_vl].begin() + nrow, 
                            tmp_val_refv[pos_vl].end());
            tmp_val_refv[pos_vl].shrink_to_fit();
          };
          bool_v.shrink_to_fit();
          for (i2 = 0; i2 < matr_idx[3].size(); i2 += 1) {
            pos_vl = matr_idx[3][i2];
            pos_vl2 = (nrow + 1) * i2;
            int_v.erase(int_v.begin() + pos_vl2, 
                            int_v.begin() + pos_vl2 + delta_col);
            tmp_val_refv[pos_vl].erase(tmp_val_refv[pos_vl].begin() + nrow, 
                            tmp_val_refv[pos_vl].end());
            tmp_val_refv[pos_vl].shrink_to_fit();
          };
          int_v.shrink_to_fit();
          for (i2 = 0; i2 < matr_idx[4].size(); i2 += 1) {
            pos_vl = matr_idx[4][i2];
            pos_vl2 = (nrow + 1) * i2;
            uint_v.erase(uint_v.begin() + pos_vl2, 
                            uint_v.begin() + pos_vl2 + delta_col);
            tmp_val_refv[pos_vl].erase(tmp_val_refv[pos_vl].begin() + nrow, 
                            tmp_val_refv[pos_vl].end());
            tmp_val_refv[pos_vl].shrink_to_fit();
          };
          uint_v.shrink_to_fit();
          for (i2 = 0; i2 < matr_idx[5].size(); i2 += 1) {
            pos_vl = matr_idx[5][i2];
            pos_vl2 = (nrow + 1) * i2;
            dbl_v.erase(dbl_v.begin() + pos_vl2, 
                            dbl_v.begin() + pos_vl2 + delta_col);
            tmp_val_refv[pos_vl].erase(tmp_val_refv[pos_vl].begin() + nrow, 
                            tmp_val_refv[pos_vl].end());
            tmp_val_refv[pos_vl].shrink_to_fit();
          };
          dbl_v.shrink_to_fit();
    
        };
    
        void merge_inner(Dataframe &obj1, Dataframe &obj2, bool colname, unsigned int &key1, unsigned int &key2) {
          const unsigned int& ncol1 = obj1.get_ncol();
          const unsigned int& ncol2 = obj2.get_ncol();
          std::vector<std::string> cur_vstr;
          ncol = ncol1 + ncol2;
          unsigned int i;
          unsigned int i2;
          unsigned int i3;
          const std::vector<std::string>& name1 = obj1.get_colname();
          const std::vector<std::string>& name2 = obj2.get_colname();
          if (colname) {
            name_v.resize(ncol);
            if (name1.size() > 0) {
              for (i = 0; i < name1.size(); ++i) {
                name_v.push_back(name1[i]);
              };
            };
            if (name2.size() > 0) {
              for (i = 0; i < name2.size(); ++i) {
                name_v.push_back(name2[i]);
              };
            };
          };
          tmp_val_refv.reserve(ncol);
          for (i = 0; i < ncol; ++i) {
            tmp_val_refv.push_back(cur_vstr);
          };
          const std::vector<char>& type1 = obj1.get_typecol();
          const std::vector<char>& type2 = obj2.get_typecol();
          type_refv.reserve(ncol);
          for (i = 0; i < ncol1; ++i) {
            type_refv.push_back(type1[i]);
          };
          for (i = 0; i < ncol2; ++i) {
            type_refv.push_back(type2[i]);
          };
          const auto& tmp1 = obj1.get_tmp_val_refv();
          const auto& tmp2 = obj2.get_tmp_val_refv();
          const std::vector<std::string>& col1 = tmp1[key1];
          const std::vector<std::string>& col2 = tmp2[key2];
          std::unordered_multimap<std::string, size_t> b_index;
          for (size_t j = 0; j < col2.size(); ++j) {
            b_index.insert({col2[j], j});
          };
          for (size_t i = 0; i < col1.size(); ++i) {
            auto range = b_index.equal_range(col1[i]);
            for (auto it = range.first; it != range.second; ++it) {
              nrow += 1;
              size_t idx = it->second;
              for (i2 = 0; i2 < ncol1; i2 += 1) {
                tmp_val_refv[i2].push_back(tmp1[i2][i]);
              };
              for (i2 = 0; i2 < ncol2; i2 += 1) {
                tmp_val_refv[ncol1 + i2].push_back(tmp2[i2][idx]);
              };
            };
          };
          type_classification();
        };
    
        void transform_merge_inner2(Dataframe &obj, 
                        unsigned int &key1, 
                        unsigned int &key2) {
          const unsigned int& ncol2 = obj.get_ncol();
          std::vector<std::string> cur_vstr(nrow);
          unsigned int nrow2 = nrow;
          //const unsigned int& nrowb = obj.get_nrow();
          nrow = 0;
          unsigned int i;
          unsigned int i2;
          unsigned int i3;
          const std::vector<std::string>& name2 = obj.get_colname();
          if (name_v.size() > 0) {
            name_v.insert(name_v.end(), name2.begin(), name2.end()); 
          };
          tmp_val_refv.reserve(ncol + ncol2);
          for (i = 0; i < ncol2; ++i) {
            tmp_val_refv.push_back(cur_vstr);
          };
          const std::vector<std::vector<unsigned int>>& matr_idx2 = obj.get_matr_idx();
          const std::vector<char>& type2 = obj.get_typecol();
          type_refv.insert(type_refv.end(), type2.begin(), type2.end());
          const auto& tmp2 = obj.get_tmp_val_refv();
          const std::vector<std::string>& col1 = tmp_val_refv[key1];
          const std::vector<std::string>& col2 = tmp2[key2];
          
          //std::unordered_map<std::string, size_t> b_index; // standard map (slower)
          ankerl::unordered_dense::map<std::string, size_t> b_index;
          for (size_t j = 0; j < col2.size(); ++j) {
            b_index.insert({col2[j], j});
          };
    
          std::vector<std::string> str_v2 = obj.get_str_vec();
          std::vector<char> chr_v2 = obj.get_chr_vec();
          std::vector<bool> bool_v2 = obj.get_bool_vec();
          std::vector<int> int_v2 = obj.get_int_vec();
          std::vector<unsigned int> uint_v2 = obj.get_uint_vec();
          std::vector<double> dbl_v2 = obj.get_dbl_vec();
    
          std::vector<std::string> tmp_str_v(nrow2);
          std::vector<char> tmp_chr_v(nrow2);
          std::vector<bool> tmp_bool_v(nrow2);
          std::vector<int> tmp_int_v(nrow2);
          std::vector<unsigned int> tmp_uint_v(nrow2);
          std::vector<double> tmp_dbl_v(nrow2);
    
          unsigned int pos_val;
    
          unsigned int pre_str_val = str_v.size() / nrow2;
          unsigned int pre_chr_val = chr_v.size() / nrow2;
          unsigned int pre_bool_val = bool_v.size() / nrow2;
          unsigned int pre_int_val = int_v.size() / nrow2;
          unsigned int pre_uint_val = uint_v.size() / nrow2;
          unsigned int pre_dbl_val = dbl_v.size() / nrow2;
    
          for (auto& el : matr_idx[0]) {
            str_v.insert(str_v.end(), tmp_str_v.begin(), tmp_str_v.end());
          };
          for (auto& el : matr_idx[1]) {
            chr_v.insert(chr_v.end(), tmp_chr_v.begin(), tmp_chr_v.end());
          };
          for (auto& el : matr_idx[2]) {
            bool_v.insert(bool_v.end(), tmp_bool_v.begin(), tmp_bool_v.end());
          };
          for (auto& el : matr_idx[3]) {
            int_v.insert(int_v.end(), tmp_int_v.begin(), tmp_int_v.end());
          };
          for (auto& el : matr_idx[4]) {
            uint_v.insert(uint_v.end(), tmp_uint_v.begin(), tmp_uint_v.end());
          };
          for (auto& el : matr_idx[5]) {
            dbl_v.insert(dbl_v.end(), tmp_dbl_v.begin(), tmp_dbl_v.end());
          };
    
          std::vector<std::vector<unsigned int>> matr_idx2b = matr_idx2;
          for (auto& el : matr_idx2b) {
            for (auto& el2 : el) {
              el2 += ncol;
            };
          };
    
          matr_idx[0].insert(matr_idx[0].end(), 
                             matr_idx2b[0].begin(), 
                             matr_idx2b[0].end());
          matr_idx[1].insert(matr_idx[1].end(), 
                             matr_idx2b[1].begin(), 
                             matr_idx2b[1].end());
          matr_idx[2].insert(matr_idx[2].end(), 
                             matr_idx2b[2].begin(), 
                             matr_idx2b[2].end());
          matr_idx[3].insert(matr_idx[3].end(), 
                             matr_idx2b[3].begin(), 
                             matr_idx2b[3].end());
          matr_idx[4].insert(matr_idx[4].end(), 
                             matr_idx2b[4].begin(), 
                             matr_idx2b[4].end());
          matr_idx[5].insert(matr_idx[5].end(), 
                             matr_idx2b[5].begin(), 
                             matr_idx2b[5].end());
    
          for (size_t i = 0; i < col1.size(); ++i) {
            auto it = b_index.find(col1[i]);
            if (it != b_index.end()) {
              size_t idx = it->second;
              for (i2 = 0; i2 < matr_idx[0].size(); i2 += 1) {
                str_v[nrow2 * i2 + nrow] = str_v[nrow2 * i2 + i];
                pos_val = matr_idx[0][i2];
                tmp_val_refv[pos_val][nrow] = tmp_val_refv[pos_val][i];
              };
              for (i2 = 0; i2 < matr_idx[1].size(); i2 += 1) {
                chr_v[nrow2 * i2 + nrow] = chr_v[nrow2 * i2 + i];
                pos_val = matr_idx[1][i2];
                tmp_val_refv[pos_val][nrow] = tmp_val_refv[pos_val][i];
              };
              for (i2 = 0; i2 < matr_idx[2].size(); i2 += 1) {
                bool_v[nrow2 * i2 + nrow] = bool_v[nrow2 * i2 + i];
                pos_val = matr_idx[2][i2];
                tmp_val_refv[pos_val][nrow] = tmp_val_refv[pos_val][i];
              };
              for (i2 = 0; i2 < matr_idx[3].size(); i2 += 1) {
                int_v[nrow2 * i2 + nrow] = int_v[nrow2 * i2 + i];
                pos_val = matr_idx[3][i2];
                tmp_val_refv[pos_val][nrow] = tmp_val_refv[pos_val][i];
              };
              for (i2 = 0; i2 < matr_idx[4].size(); i2 += 1) {
                uint_v[nrow2 * i2 + nrow] = uint_v[nrow2 * i2 + i];
                pos_val = matr_idx[4][i2];
                tmp_val_refv[pos_val][nrow] = tmp_val_refv[pos_val][i];
              };
              for (i2 = 0; i2 < matr_idx[5].size(); i2 += 1) {
                dbl_v[nrow2 * i2 + nrow] = dbl_v[nrow2 * i2 + i];
                pos_val = matr_idx[5][i2];
                tmp_val_refv[pos_val][nrow] = tmp_val_refv[pos_val][i];
              };
              for (i2 = 0; i2 < matr_idx2[0].size(); i2 += 1) {
                pos_val = pre_str_val + i2;
                str_v[nrow2 * pos_val + nrow] = str_v[nrow2 * pos_val + idx];
                pos_val = matr_idx2[0][i2];
                tmp_val_refv[ncol + pos_val][nrow] = tmp2[pos_val][idx];
              };
              for (i2 = 0; i2 < matr_idx2[1].size(); i2 += 1) {
                pos_val = pre_chr_val + i2;
                chr_v[nrow2 * pos_val + nrow] = chr_v[nrow2 * pos_val + idx];
                pos_val = matr_idx2[1][i2];
                tmp_val_refv[ncol + pos_val][nrow] = tmp2[pos_val][idx];
              };
              for (i2 = 0; i2 < matr_idx2[2].size(); i2 += 1) {
                pos_val = pre_bool_val + i2;
                bool_v[nrow2 * pos_val + nrow] = bool_v[nrow2 * pos_val + idx];
                pos_val = matr_idx2[2][i2];
                tmp_val_refv[ncol + pos_val][nrow] = tmp2[pos_val][idx];
              };
              for (i2 = 0; i2 < matr_idx2[3].size(); i2 += 1) {
                pos_val = pre_int_val + i2;
                int_v[nrow2 * pos_val + nrow] = int_v[nrow2 * pos_val + idx];
                pos_val = matr_idx2[3][i2];
                tmp_val_refv[ncol + pos_val][nrow] = tmp2[pos_val][idx];
              };
              for (i2 = 0; i2 < matr_idx2[4].size(); i2 += 1) {
                pos_val = pre_uint_val + i2;
                uint_v[nrow2 * pos_val + nrow] = uint_v[nrow2 * pos_val + idx];
                pos_val = matr_idx2[4][i2];
                tmp_val_refv[ncol + pos_val][nrow] = tmp2[pos_val][idx];
              };
              for (i2 = 0; i2 < matr_idx2[5].size(); i2 += 1) {
                pos_val = pre_dbl_val + i2;
                dbl_v[nrow2 * pos_val + nrow] = dbl_v[nrow2 * pos_val + idx];
                pos_val = matr_idx2[5][i2];
                tmp_val_refv[ncol + pos_val][nrow] = tmp2[pos_val][idx];
              };
              nrow += 1;
            };
          };
          ncol += ncol2;
        };
    
        void transform_merge_inner2_clean(Dataframe &obj, 
                        unsigned int &key1, 
                        unsigned int &key2) {
          const unsigned int& ncol2 = obj.get_ncol();
          std::vector<std::string> cur_vstr(nrow);
          unsigned int nrow2 = nrow;
          nrow = 0;
          unsigned int i;
          unsigned int i2;
          unsigned int i3;
          const std::vector<std::string>& name2 = obj.get_colname();
          if (name_v.size() > 0) {
            name_v.insert(name_v.end(), name2.begin(), name2.end()); 
          };
          tmp_val_refv.reserve(ncol + ncol2);
          for (i = 0; i < ncol2; ++i) {
            tmp_val_refv.push_back(cur_vstr);
          };
          const std::vector<std::vector<unsigned int>>& matr_idx2 = obj.get_matr_idx();
          const std::vector<char>& type2 = obj.get_typecol();
          type_refv.insert(type_refv.end(), type2.begin(), type2.end());
          const auto& tmp2 = obj.get_tmp_val_refv();
          const std::vector<std::string>& col1 = tmp_val_refv[key1];
          const std::vector<std::string>& col2 = tmp2[key2];
          
          //std::unordered_map<std::string, size_t> b_index; // standard map (slower)
          ankerl::unordered_dense::map<std::string, size_t> b_index;
          for (size_t j = 0; j < col2.size(); ++j) {
            b_index.insert({col2[j], j});
          };
    
          std::vector<std::string> str_v2 = obj.get_str_vec();
          std::vector<char> chr_v2 = obj.get_chr_vec();
          std::vector<bool> bool_v2 = obj.get_bool_vec();
          std::vector<int> int_v2 = obj.get_int_vec();
          std::vector<unsigned int> uint_v2 = obj.get_uint_vec();
          std::vector<double> dbl_v2 = obj.get_dbl_vec();
    
          std::vector<std::string> tmp_str_v(nrow2);
          std::vector<char> tmp_chr_v(nrow2);
          std::vector<bool> tmp_bool_v(nrow2);
          std::vector<int> tmp_int_v(nrow2);
          std::vector<unsigned int> tmp_uint_v(nrow2);
          std::vector<double> tmp_dbl_v(nrow2);
    
          unsigned int pos_val;
    
          unsigned int pre_str_val = str_v.size() / nrow2;
          unsigned int pre_chr_val = chr_v.size() / nrow2;
          unsigned int pre_bool_val = bool_v.size() / nrow2;
          unsigned int pre_int_val = int_v.size() / nrow2;
          unsigned int pre_uint_val = uint_v.size() / nrow2;
          unsigned int pre_dbl_val = dbl_v.size() / nrow2;
    
          for (auto& el : matr_idx[0]) {
            str_v.insert(str_v.end(), tmp_str_v.begin(), tmp_str_v.end());
          };
          for (auto& el : matr_idx[1]) {
            chr_v.insert(chr_v.end(), tmp_chr_v.begin(), tmp_chr_v.end());
          };
          for (auto& el : matr_idx[2]) {
            bool_v.insert(bool_v.end(), tmp_bool_v.begin(), tmp_bool_v.end());
          };
          for (auto& el : matr_idx[3]) {
            int_v.insert(int_v.end(), tmp_int_v.begin(), tmp_int_v.end());
          };
          for (auto& el : matr_idx[4]) {
            uint_v.insert(uint_v.end(), tmp_uint_v.begin(), tmp_uint_v.end());
          };
          for (auto& el : matr_idx[5]) {
            dbl_v.insert(dbl_v.end(), tmp_dbl_v.begin(), tmp_dbl_v.end());
          };
    
          std::vector<std::vector<unsigned int>> matr_idx2b = matr_idx2;
          for (auto& el : matr_idx2b) {
            for (auto& el2 : el) {
              el2 += ncol;
            };
          };
    
          matr_idx[0].insert(matr_idx[0].end(), 
                             matr_idx2b[0].begin(), 
                             matr_idx2b[0].end());
          matr_idx[1].insert(matr_idx[1].end(), 
                             matr_idx2b[1].begin(), 
                             matr_idx2b[1].end());
          matr_idx[2].insert(matr_idx[2].end(), 
                             matr_idx2b[2].begin(), 
                             matr_idx2b[2].end());
          matr_idx[3].insert(matr_idx[3].end(), 
                             matr_idx2b[3].begin(), 
                             matr_idx2b[3].end());
          matr_idx[4].insert(matr_idx[4].end(), 
                             matr_idx2b[4].begin(), 
                             matr_idx2b[4].end());
          matr_idx[5].insert(matr_idx[5].end(), 
                             matr_idx2b[5].begin(), 
                             matr_idx2b[5].end());
    
          for (size_t i = 0; i < col1.size(); ++i) {
            auto it = b_index.find(col1[i]);
            if (it != b_index.end()) {
              size_t idx = it->second;
              for (i2 = 0; i2 < matr_idx[0].size(); i2 += 1) {
                str_v[nrow2 * i2 + nrow] = str_v[nrow2 * i2 + i];
                pos_val = matr_idx[0][i2];
                tmp_val_refv[pos_val][nrow] = tmp_val_refv[pos_val][i];
              };
              for (i2 = 0; i2 < matr_idx[1].size(); i2 += 1) {
                chr_v[nrow2 * i2 + nrow] = chr_v[nrow2 * i2 + i];
                pos_val = matr_idx[1][i2];
                tmp_val_refv[pos_val][nrow] = tmp_val_refv[pos_val][i];
              };
              for (i2 = 0; i2 < matr_idx[2].size(); i2 += 1) {
                bool_v[nrow2 * i2 + nrow] = bool_v[nrow2 * i2 + i];
                pos_val = matr_idx[2][i2];
                tmp_val_refv[pos_val][nrow] = tmp_val_refv[pos_val][i];
              };
              for (i2 = 0; i2 < matr_idx[3].size(); i2 += 1) {
                int_v[nrow2 * i2 + nrow] = int_v[nrow2 * i2 + i];
                pos_val = matr_idx[3][i2];
                tmp_val_refv[pos_val][nrow] = tmp_val_refv[pos_val][i];
              };
              for (i2 = 0; i2 < matr_idx[4].size(); i2 += 1) {
                uint_v[nrow2 * i2 + nrow] = uint_v[nrow2 * i2 + i];
                pos_val = matr_idx[4][i2];
                tmp_val_refv[pos_val][nrow] = tmp_val_refv[pos_val][i];
              };
              for (i2 = 0; i2 < matr_idx[5].size(); i2 += 1) {
                dbl_v[nrow2 * i2 + nrow] = dbl_v[nrow2 * i2 + i];
                pos_val = matr_idx[5][i2];
                tmp_val_refv[pos_val][nrow] = tmp_val_refv[pos_val][i];
              };
              for (i2 = 0; i2 < matr_idx2[0].size(); i2 += 1) {
                pos_val = pre_str_val + i2;
                str_v[nrow2 * pos_val + nrow] = str_v[nrow2 * pos_val + idx];
                pos_val = matr_idx2[0][i2];
                tmp_val_refv[ncol + pos_val][nrow] = tmp2[pos_val][idx];
              };
              for (i2 = 0; i2 < matr_idx2[1].size(); i2 += 1) {
                pos_val = pre_chr_val + i2;
                chr_v[nrow2 * pos_val + nrow] = chr_v[nrow2 * pos_val + idx];
                pos_val = matr_idx2[1][i2];
                tmp_val_refv[ncol + pos_val][nrow] = tmp2[pos_val][idx];
              };
              for (i2 = 0; i2 < matr_idx2[2].size(); i2 += 1) {
                pos_val = pre_bool_val + i2;
                bool_v[nrow2 * pos_val + nrow] = bool_v[nrow2 * pos_val + idx];
                pos_val = matr_idx2[2][i2];
                tmp_val_refv[ncol + pos_val][nrow] = tmp2[pos_val][idx];
              };
              for (i2 = 0; i2 < matr_idx2[3].size(); i2 += 1) {
                pos_val = pre_int_val + i2;
                int_v[nrow2 * pos_val + nrow] = int_v[nrow2 * pos_val + idx];
                pos_val = matr_idx2[3][i2];
                tmp_val_refv[ncol + pos_val][nrow] = tmp2[pos_val][idx];
              };
              for (i2 = 0; i2 < matr_idx2[4].size(); i2 += 1) {
                pos_val = pre_uint_val + i2;
                uint_v[nrow2 * pos_val + nrow] = uint_v[nrow2 * pos_val + idx];
                pos_val = matr_idx2[4][i2];
                tmp_val_refv[ncol + pos_val][nrow] = tmp2[pos_val][idx];
              };
              for (i2 = 0; i2 < matr_idx2[5].size(); i2 += 1) {
                pos_val = pre_dbl_val + i2;
                dbl_v[nrow2 * pos_val + nrow] = dbl_v[nrow2 * pos_val + idx];
                pos_val = matr_idx2[5][i2];
                tmp_val_refv[ncol + pos_val][nrow] = tmp2[pos_val][idx];
              };
              nrow += 1;
            };
          };
          ncol += ncol2;
          unsigned int delta_col = nrow2 - nrow;
          for (i2 = 0; i2 < matr_idx[0].size(); i2 += 1) {
            pos_val = matr_idx[0][i2];
            tmp_val_refv[pos_val].erase(tmp_val_refv[pos_val].begin() + nrow, 
                            tmp_val_refv[pos_val].end());
            tmp_val_refv[pos_val].shrink_to_fit();
            pos_val = (nrow + 1) * i2;
            str_v.erase(str_v.begin() + pos_val, 
                            str_v.begin() + pos_val + delta_col);
          };
          str_v.shrink_to_fit();
          for (i2 = 0; i2 < matr_idx[1].size(); i2 += 1) {
            pos_val = matr_idx[1][i2];
            tmp_val_refv[pos_val].erase(tmp_val_refv[pos_val].begin() + nrow, 
                            tmp_val_refv[pos_val].end());
            tmp_val_refv[pos_val].shrink_to_fit();
            pos_val = (nrow + 1) * i2;
            chr_v.erase(chr_v.begin() + pos_val, 
                            chr_v.begin() + pos_val + delta_col);
          };
          chr_v.shrink_to_fit();
          for (i2 = 0; i2 < matr_idx[2].size(); i2 += 1) {
            pos_val = matr_idx[2][i2];
            tmp_val_refv[pos_val].erase(tmp_val_refv[pos_val].begin() + nrow, 
                            tmp_val_refv[pos_val].end());
            tmp_val_refv[pos_val].shrink_to_fit();
            pos_val = (nrow + 1) * i2;
            bool_v.erase(bool_v.begin() + pos_val, 
                            bool_v.begin() + pos_val + delta_col);
          };
          bool_v.shrink_to_fit();
          for (i2 = 0; i2 < matr_idx[3].size(); i2 += 1) {
            pos_val = matr_idx[3][i2];
            tmp_val_refv[pos_val].erase(tmp_val_refv[pos_val].begin() + nrow, 
                            tmp_val_refv[pos_val].end());
            tmp_val_refv[pos_val].shrink_to_fit();
            pos_val = (nrow + 1) * i2;
            int_v.erase(int_v.begin() + pos_val, 
                            int_v.begin() + pos_val + delta_col);
          };
          int_v.shrink_to_fit();
          for (i2 = 0; i2 < matr_idx[4].size(); i2 += 1) {
            pos_val = matr_idx[4][i2];
            tmp_val_refv[pos_val].erase(tmp_val_refv[pos_val].begin() + nrow, 
                            tmp_val_refv[pos_val].end());
            tmp_val_refv[pos_val].shrink_to_fit();
            pos_val = (nrow + 1) * i2;
            uint_v.erase(uint_v.begin() + pos_val, 
                            uint_v.begin() + pos_val + delta_col);
          };
          uint_v.shrink_to_fit();
          for (i2 = 0; i2 < matr_idx[5].size(); i2 += 1) {
            pos_val = matr_idx[5][i2];
            tmp_val_refv[pos_val].erase(tmp_val_refv[pos_val].begin() + nrow, 
                            tmp_val_refv[pos_val].end());
            tmp_val_refv[pos_val].shrink_to_fit();
            pos_val = (nrow + 1) * i2;
            dbl_v.erase(dbl_v.begin() + pos_val, 
                            dbl_v.begin() + pos_val + delta_col);
          };
          dbl_v.shrink_to_fit();
        };
    
        void merge_inner2(Dataframe &obj1, Dataframe &obj2, bool colname, unsigned int &key1, unsigned int &key2) {
          const unsigned int& ncol1 = obj1.get_ncol();
          const unsigned int& ncol2 = obj2.get_ncol();
          std::vector<std::string> cur_vstr;
          ncol = ncol1 + ncol2;
          unsigned int i;
          unsigned int i2;
          unsigned int i3;
          const std::vector<std::string>& name1 = obj1.get_colname();
          const std::vector<std::string>& name2 = obj2.get_colname();
          if (colname) {
            name_v.resize(ncol);
            if (name1.size() > 0) {
              for (i = 0; i < name1.size(); ++i) {
                name_v.push_back(name1[i]);
              };
            };
            if (name2.size() > 0) {
              for (i = 0; i < name2.size(); ++i) {
                name_v.push_back(name2[i]);
              };
            };
          };
          tmp_val_refv.reserve(ncol);
          for (i = 0; i < ncol; ++i) {
            tmp_val_refv.push_back(cur_vstr);
          };
          const std::vector<char>& type1 = obj1.get_typecol();
          const std::vector<char>& type2 = obj2.get_typecol();
          const auto& tmp1 = obj1.get_tmp_val_refv();
          const auto& tmp2 = obj2.get_tmp_val_refv();
          const std::vector<std::string>& col1 = tmp1[key1];
          const std::vector<std::string>& col2 = tmp2[key2];
          //std::unordered_map<std::string, size_t> b_index; // standard map (slower)
          ankerl::unordered_dense::map<std::string, size_t> b_index;
          for (size_t j = 0; j < col2.size(); ++j) {
            b_index.insert({col2[j], j});
          };
          for (size_t i = 0; i < col1.size(); ++i) {
            auto it = b_index.find(col1[i]);
            if (it != b_index.end()) {
              nrow += 1;
              size_t idx = it->second;
              for (i2 = 0; i2 < ncol1; i2 += 1) {
                tmp_val_refv[i2].push_back(tmp1[i2][i]);
              };
              for (i2 = 0; i2 < ncol2; i2 += 1) {
                tmp_val_refv[ncol1 + i2].push_back(tmp2[i2][idx]);
              };
            };
          };
          type_classification();
        };
    
        void merge_excluding(Dataframe &obj1, 
                             Dataframe &obj2, 
                             bool colname, 
                             unsigned int &key1, 
                             unsigned int &key2,
                             std::string default_str = "NA",
                             std::string default_chr = " ",
                             std::string default_bool = "0",
                             std::string default_int = "0",
                             std::string default_uint = "0",
                             std::string default_dbl = "0") {
          const unsigned int& ncol1 = obj1.get_ncol();
          const unsigned int& ncol2 = obj2.get_ncol();
          std::vector<std::string> cur_vstr;
          ncol = ncol1 + ncol2;
          unsigned int i;
          unsigned int i2;
          unsigned int i3;
          const std::vector<std::vector<unsigned int>>& matr_idx2 = obj2.get_matr_idx();
          const std::vector<std::string>& name1 = obj1.get_colname();
          const std::vector<std::string>& name2 = obj2.get_colname();
          if (colname) {
            name_v.resize(ncol);
            if (name1.size() > 0) {
              for (i = 0; i < name1.size(); ++i) {
                name_v.push_back(name1[i]);
              };
            };
            if (name2.size() > 0) {
              for (i = 0; i < name2.size(); ++i) {
                name_v.push_back(name2[i]);
              };
            };
          };
          tmp_val_refv.reserve(ncol);
          for (i = 0; i < ncol; ++i) {
            tmp_val_refv.push_back(cur_vstr);
          };
          const std::vector<char>& type1 = obj1.get_typecol();
          const std::vector<char>& type2 = obj2.get_typecol();
          const auto& tmp1 = obj1.get_tmp_val_refv();
          const auto& tmp2 = obj2.get_tmp_val_refv();
          const std::vector<std::string>& col1 = tmp1[key1];
          const std::vector<std::string>& col2 = tmp2[key2];
          std::unordered_multimap<std::string, size_t> b_index;
          for (size_t j = 0; j < col2.size(); ++j) {
            b_index.insert({col2[j], j});
          };
          for (size_t i = 0; i < col1.size(); ++i) {
            auto range = b_index.equal_range(col1[i]);
            if (range.first == range.second) {
              nrow += 1;
              for (i2 = 0; i2 < ncol1; i2 += 1) {
                tmp_val_refv[i2].push_back(tmp1[i2][i]);
              };
              for (auto& el : matr_idx2[0]) {
                tmp_val_refv[ncol1 + el].push_back(default_str);
              };
              for (auto& el : matr_idx2[1]) {
                tmp_val_refv[ncol1 + el].push_back(default_chr);
              };
              for (auto& el : matr_idx2[2]) {
                tmp_val_refv[ncol1 + el].push_back(default_bool);
              };
              for (auto& el : matr_idx2[3]) {
                tmp_val_refv[ncol1 + el].push_back(default_int);
              };
              for (auto& el : matr_idx2[4]) {
                tmp_val_refv[ncol1 + el].push_back(default_uint);
              };
              for (auto& el : matr_idx2[5]) {
                tmp_val_refv[ncol1 + el].push_back(default_dbl);
              };
            };
          };
          type_classification();
        };
        
        void transform_merge_excluding(Dataframe &obj, 
                        unsigned int &key1, 
                        unsigned int &key2,
                        std::string default_str = "NA",
                        char default_chr = ' ',
                        bool default_bool = 0,
                        int default_int = 0,
                        unsigned int default_uint = 0,
                        double default_dbl = 0) {
          const unsigned int& ncol2 = obj.get_ncol();
          std::vector<std::string> cur_vstr(nrow);
          unsigned int nrow2 = nrow;
          nrow = 0;
          unsigned int i;
          unsigned int i2;
          unsigned int i3;
          const std::vector<std::string>& name2 = obj.get_colname();
          if (name_v.size() > 0) {
            name_v.insert(name_v.end(), name2.begin(), name2.end()); 
          };
          tmp_val_refv.reserve(ncol + ncol2);
          for (i = 0; i < ncol2; ++i) {
            tmp_val_refv.push_back(cur_vstr);
          };
          const std::vector<std::vector<unsigned int>>& matr_idx2 = obj.get_matr_idx();
          const std::vector<char>& type2 = obj.get_typecol();
          type_refv.insert(type_refv.end(), type2.begin(), type2.end());
          const auto& tmp2 = obj.get_tmp_val_refv();
          const std::vector<std::string>& col1 = tmp_val_refv[key1];
          const std::vector<std::string>& col2 = tmp2[key2];
          
          //std::unordered_map<std::string, size_t> b_index; // standard map (slower)
          ankerl::unordered_dense::map<std::string, size_t> b_index;
          for (size_t j = 0; j < col2.size(); ++j) {
            b_index.insert({col2[j], j});
          };
    
          std::vector<std::string> str_v2 = obj.get_str_vec();
          std::vector<char> chr_v2 = obj.get_chr_vec();
          std::vector<bool> bool_v2 = obj.get_bool_vec();
          std::vector<int> int_v2 = obj.get_int_vec();
          std::vector<unsigned int> uint_v2 = obj.get_uint_vec();
          std::vector<double> dbl_v2 = obj.get_dbl_vec();
    
          std::vector<std::string> tmp_str_v(nrow2);
          std::vector<char> tmp_chr_v(nrow2);
          std::vector<bool> tmp_bool_v(nrow2);
          std::vector<int> tmp_int_v(nrow2);
          std::vector<unsigned int> tmp_uint_v(nrow2);
          std::vector<double> tmp_dbl_v(nrow2);
    
          unsigned int pos_val;
    
          unsigned int pre_str_val = str_v.size() / nrow2;
          unsigned int pre_chr_val = chr_v.size() / nrow2;
          unsigned int pre_bool_val = bool_v.size() / nrow2;
          unsigned int pre_int_val = int_v.size() / nrow2;
          unsigned int pre_uint_val = uint_v.size() / nrow2;
          unsigned int pre_dbl_val = dbl_v.size() / nrow2;
    
          for (auto& el : matr_idx[0]) {
            str_v.insert(str_v.end(), tmp_str_v.begin(), tmp_str_v.end());
          };
          for (auto& el : matr_idx[1]) {
            chr_v.insert(chr_v.end(), tmp_chr_v.begin(), tmp_chr_v.end());
          };
          for (auto& el : matr_idx[2]) {
            bool_v.insert(bool_v.end(), tmp_bool_v.begin(), tmp_bool_v.end());
          };
          for (auto& el : matr_idx[3]) {
            int_v.insert(int_v.end(), tmp_int_v.begin(), tmp_int_v.end());
          };
          for (auto& el : matr_idx[4]) {
            uint_v.insert(uint_v.end(), tmp_uint_v.begin(), tmp_uint_v.end());
          };
          for (auto& el : matr_idx[5]) {
            dbl_v.insert(dbl_v.end(), tmp_dbl_v.begin(), tmp_dbl_v.end());
          };
    
          std::vector<std::vector<unsigned int>> matr_idx2b = matr_idx2;
          for (auto& el : matr_idx2b) {
            for (auto& el2 : el) {
              el2 += ncol;
            };
          };
    
          matr_idx[0].insert(matr_idx[0].end(), 
                             matr_idx2b[0].begin(), 
                             matr_idx2b[0].end());
          matr_idx[1].insert(matr_idx[1].end(), 
                             matr_idx2b[1].begin(), 
                             matr_idx2b[1].end());
          matr_idx[2].insert(matr_idx[2].end(), 
                             matr_idx2b[2].begin(), 
                             matr_idx2b[2].end());
          matr_idx[3].insert(matr_idx[3].end(), 
                             matr_idx2b[3].begin(), 
                             matr_idx2b[3].end());
          matr_idx[4].insert(matr_idx[4].end(), 
                             matr_idx2b[4].begin(), 
                             matr_idx2b[4].end());
          matr_idx[5].insert(matr_idx[5].end(), 
                             matr_idx2b[5].begin(), 
                             matr_idx2b[5].end());
    
          for (size_t i = 0; i < col1.size(); ++i) { 
            auto it = b_index.find(col1[i]);
    
            if (it == b_index.end()) {
    
              for (i2 = 0; i2 < matr_idx[0].size(); i2 += 1) {
                str_v[nrow2 * i2 + nrow] = str_v[nrow2 * i2 + i];
                pos_val = matr_idx[0][i2];
                tmp_val_refv[pos_val][nrow] = tmp_val_refv[pos_val][i];
              };
              for (i2 = 0; i2 < matr_idx[1].size(); i2 += 1) {
                chr_v[nrow2 * i2 + nrow] = chr_v[nrow2 * i2 + i];
                pos_val = matr_idx[1][i2];
                tmp_val_refv[pos_val][nrow] = tmp_val_refv[pos_val][i];
              };
              for (i2 = 0; i2 < matr_idx[2].size(); i2 += 1) {
                bool_v[nrow2 * i2 + nrow] = bool_v[nrow2 * i2 + i];
                pos_val = matr_idx[2][i2];
                tmp_val_refv[pos_val][nrow] = tmp_val_refv[pos_val][i];
              };
              for (i2 = 0; i2 < matr_idx[3].size(); i2 += 1) {
                int_v[nrow2 * i2 + nrow] = int_v[nrow2 * i2 + i];
                pos_val = matr_idx[3][i2];
                tmp_val_refv[pos_val][nrow] = tmp_val_refv[pos_val][i];
              };
              for (i2 = 0; i2 < matr_idx[4].size(); i2 += 1) {
                uint_v[nrow2 * i2 + nrow] = uint_v[nrow2 * i2 + i];
                pos_val = matr_idx[4][i2];
                tmp_val_refv[pos_val][nrow] = tmp_val_refv[pos_val][i];
              };
              for (i2 = 0; i2 < matr_idx[5].size(); i2 += 1) {
                dbl_v[nrow2 * i2 + nrow] = dbl_v[nrow2 * i2 + i];
                pos_val = matr_idx[5][i2];
                tmp_val_refv[pos_val][nrow] = tmp_val_refv[pos_val][i];
              };
              for (i2 = 0; i2 < matr_idx2[0].size(); i2 += 1) {
                pos_val = pre_str_val + i2;
                str_v[nrow2 * pos_val + nrow] = default_str;
                pos_val = matr_idx2[0][i2];
                tmp_val_refv[ncol + pos_val][nrow] = default_str;
              };
              for (i2 = 0; i2 < matr_idx2[1].size(); i2 += 1) {
                pos_val = pre_chr_val + i2;
                chr_v[nrow2 * pos_val + nrow] = default_chr;
                pos_val = matr_idx2[1][i2];
                tmp_val_refv[ncol + pos_val][nrow] = " ";
              };
              for (i2 = 0; i2 < matr_idx2[2].size(); i2 += 1) {
                pos_val = pre_bool_val + i2;
                bool_v[nrow2 * pos_val + nrow] = default_bool;
                pos_val = matr_idx2[2][i2];
                tmp_val_refv[ncol + pos_val][nrow] = std::to_string(default_bool);
              };
              for (i2 = 0; i2 < matr_idx2[3].size(); i2 += 1) {
                pos_val = pre_int_val + i2;
                int_v[nrow2 * pos_val + nrow] = default_int;
                pos_val = matr_idx2[3][i2];
                tmp_val_refv[ncol + pos_val][nrow] = std::to_string(default_int);
              };
              for (i2 = 0; i2 < matr_idx2[4].size(); i2 += 1) {
                pos_val = pre_uint_val + i2;
                uint_v[nrow2 * pos_val + nrow] = default_uint;
                pos_val = matr_idx2[4][i2];
                tmp_val_refv[ncol + pos_val][nrow] = std::to_string(default_uint);
              };
              for (i2 = 0; i2 < matr_idx2[5].size(); i2 += 1) {
                pos_val = pre_dbl_val + i2;
                dbl_v[nrow2 * pos_val + nrow] = default_dbl;
                pos_val = matr_idx2[5][i2];
                tmp_val_refv[ncol + pos_val][nrow] = std::to_string(default_dbl);
              };
              nrow += 1;
            };
          };
          ncol += ncol2;
        };
    
        void transform_merge_excluding_clean(Dataframe &obj, 
                        unsigned int &key1, 
                        unsigned int &key2,
                        std::string default_str = "NA",
                        char default_chr = ' ',
                        bool default_bool = 0,
                        int default_int = 0,
                        unsigned int default_uint = 0,
                        double default_dbl = 0) {
          const unsigned int& ncol2 = obj.get_ncol();
          std::vector<std::string> cur_vstr(nrow);
          unsigned int nrow2 = nrow;
          nrow = 0;
          unsigned int i;
          unsigned int i2;
          unsigned int i3;
          const std::vector<std::string>& name2 = obj.get_colname();
          if (name_v.size() > 0) {
            name_v.insert(name_v.end(), name2.begin(), name2.end()); 
          };
          tmp_val_refv.reserve(ncol + ncol2);
          for (i = 0; i < ncol2; ++i) {
            tmp_val_refv.push_back(cur_vstr);
          };
          const std::vector<std::vector<unsigned int>>& matr_idx2 = obj.get_matr_idx();
          const std::vector<char>& type2 = obj.get_typecol();
          type_refv.insert(type_refv.end(), type2.begin(), type2.end());
          const auto& tmp2 = obj.get_tmp_val_refv();
          const std::vector<std::string>& col1 = tmp_val_refv[key1];
          const std::vector<std::string>& col2 = tmp2[key2];
          
          //std::unordered_map<std::string, size_t> b_index; // standard map (slower)
          ankerl::unordered_dense::map<std::string, size_t> b_index;
          for (size_t j = 0; j < col2.size(); ++j) {
            b_index.insert({col2[j], j});
          };
    
          std::vector<std::string> str_v2 = obj.get_str_vec();
          std::vector<char> chr_v2 = obj.get_chr_vec();
          std::vector<bool> bool_v2 = obj.get_bool_vec();
          std::vector<int> int_v2 = obj.get_int_vec();
          std::vector<unsigned int> uint_v2 = obj.get_uint_vec();
          std::vector<double> dbl_v2 = obj.get_dbl_vec();
    
          std::vector<std::string> tmp_str_v(nrow2);
          std::vector<char> tmp_chr_v(nrow2);
          std::vector<bool> tmp_bool_v(nrow2);
          std::vector<int> tmp_int_v(nrow2);
          std::vector<unsigned int> tmp_uint_v(nrow2);
          std::vector<double> tmp_dbl_v(nrow2);
    
          unsigned int pos_val;
    
          unsigned int pre_str_val = str_v.size() / nrow2;
          unsigned int pre_chr_val = chr_v.size() / nrow2;
          unsigned int pre_bool_val = bool_v.size() / nrow2;
          unsigned int pre_int_val = int_v.size() / nrow2;
          unsigned int pre_uint_val = uint_v.size() / nrow2;
          unsigned int pre_dbl_val = dbl_v.size() / nrow2;
    
          for (auto& el : matr_idx[0]) {
            str_v.insert(str_v.end(), tmp_str_v.begin(), tmp_str_v.end());
          };
          for (auto& el : matr_idx[1]) {
            chr_v.insert(chr_v.end(), tmp_chr_v.begin(), tmp_chr_v.end());
          };
          for (auto& el : matr_idx[2]) {
            bool_v.insert(bool_v.end(), tmp_bool_v.begin(), tmp_bool_v.end());
          };
          for (auto& el : matr_idx[3]) {
            int_v.insert(int_v.end(), tmp_int_v.begin(), tmp_int_v.end());
          };
          for (auto& el : matr_idx[4]) {
            uint_v.insert(uint_v.end(), tmp_uint_v.begin(), tmp_uint_v.end());
          };
          for (auto& el : matr_idx[5]) {
            dbl_v.insert(dbl_v.end(), tmp_dbl_v.begin(), tmp_dbl_v.end());
          };
    
          std::vector<std::vector<unsigned int>> matr_idx2b = matr_idx2;
          for (auto& el : matr_idx2b) {
            for (auto& el2 : el) {
              el2 += ncol;
            };
          };
    
          matr_idx[0].insert(matr_idx[0].end(), 
                             matr_idx2b[0].begin(), 
                             matr_idx2b[0].end());
          matr_idx[1].insert(matr_idx[1].end(), 
                             matr_idx2b[1].begin(), 
                             matr_idx2b[1].end());
          matr_idx[2].insert(matr_idx[2].end(), 
                             matr_idx2b[2].begin(), 
                             matr_idx2b[2].end());
          matr_idx[3].insert(matr_idx[3].end(), 
                             matr_idx2b[3].begin(), 
                             matr_idx2b[3].end());
          matr_idx[4].insert(matr_idx[4].end(), 
                             matr_idx2b[4].begin(), 
                             matr_idx2b[4].end());
          matr_idx[5].insert(matr_idx[5].end(), 
                             matr_idx2b[5].begin(), 
                             matr_idx2b[5].end());
    
          for (size_t i = 0; i < col1.size(); ++i) { 
            auto it = b_index.find(col1[i]);
    
            if (it == b_index.end()) {
    
              for (i2 = 0; i2 < matr_idx[0].size(); i2 += 1) {
                str_v[nrow2 * i2 + nrow] = str_v[nrow2 * i2 + i];
                pos_val = matr_idx[0][i2];
                tmp_val_refv[pos_val][nrow] = tmp_val_refv[pos_val][i];
              };
              for (i2 = 0; i2 < matr_idx[1].size(); i2 += 1) {
                chr_v[nrow2 * i2 + nrow] = chr_v[nrow2 * i2 + i];
                pos_val = matr_idx[1][i2];
                tmp_val_refv[pos_val][nrow] = tmp_val_refv[pos_val][i];
              };
              for (i2 = 0; i2 < matr_idx[2].size(); i2 += 1) {
                bool_v[nrow2 * i2 + nrow] = bool_v[nrow2 * i2 + i];
                pos_val = matr_idx[2][i2];
                tmp_val_refv[pos_val][nrow] = tmp_val_refv[pos_val][i];
              };
              for (i2 = 0; i2 < matr_idx[3].size(); i2 += 1) {
                int_v[nrow2 * i2 + nrow] = int_v[nrow2 * i2 + i];
                pos_val = matr_idx[3][i2];
                tmp_val_refv[pos_val][nrow] = tmp_val_refv[pos_val][i];
              };
              for (i2 = 0; i2 < matr_idx[4].size(); i2 += 1) {
                uint_v[nrow2 * i2 + nrow] = uint_v[nrow2 * i2 + i];
                pos_val = matr_idx[4][i2];
                tmp_val_refv[pos_val][nrow] = tmp_val_refv[pos_val][i];
              };
              for (i2 = 0; i2 < matr_idx[5].size(); i2 += 1) {
                dbl_v[nrow2 * i2 + nrow] = dbl_v[nrow2 * i2 + i];
                pos_val = matr_idx[5][i2];
                tmp_val_refv[pos_val][nrow] = tmp_val_refv[pos_val][i];
              };
              for (i2 = 0; i2 < matr_idx2[0].size(); i2 += 1) {
                pos_val = pre_str_val + i2;
                str_v[nrow2 * pos_val + nrow] = default_str;
                pos_val = matr_idx2[0][i2];
                tmp_val_refv[ncol + pos_val][nrow] = default_str;
              };
              for (i2 = 0; i2 < matr_idx2[1].size(); i2 += 1) {
                pos_val = pre_chr_val + i2;
                chr_v[nrow2 * pos_val + nrow] = default_chr;
                pos_val = matr_idx2[1][i2];
                tmp_val_refv[ncol + pos_val][nrow] = " ";
              };
              for (i2 = 0; i2 < matr_idx2[2].size(); i2 += 1) {
                pos_val = pre_bool_val + i2;
                bool_v[nrow2 * pos_val + nrow] = default_bool;
                pos_val = matr_idx2[2][i2];
                tmp_val_refv[ncol + pos_val][nrow] = std::to_string(default_bool);
              };
              for (i2 = 0; i2 < matr_idx2[3].size(); i2 += 1) {
                pos_val = pre_int_val + i2;
                int_v[nrow2 * pos_val + nrow] = default_int;
                pos_val = matr_idx2[3][i2];
                tmp_val_refv[ncol + pos_val][nrow] = std::to_string(default_int);
              };
              for (i2 = 0; i2 < matr_idx2[4].size(); i2 += 1) {
                pos_val = pre_uint_val + i2;
                uint_v[nrow2 * pos_val + nrow] = default_uint;
                pos_val = matr_idx2[4][i2];
                tmp_val_refv[ncol + pos_val][nrow] = std::to_string(default_uint);
              };
              for (i2 = 0; i2 < matr_idx2[5].size(); i2 += 1) {
                pos_val = pre_dbl_val + i2;
                dbl_v[nrow2 * pos_val + nrow] = default_dbl;
                pos_val = matr_idx2[5][i2];
                tmp_val_refv[ncol + pos_val][nrow] = std::to_string(default_dbl);
              };
              nrow += 1;
            };
          };
          ncol += ncol2;
          unsigned int delta_col = nrow2 - nrow;
          for (i2 = 0; i2 < matr_idx[0].size(); i2 += 1) {
            pos_val = matr_idx[0][i2];
            tmp_val_refv[pos_val].erase(tmp_val_refv[pos_val].begin() + nrow, 
                            tmp_val_refv[pos_val].end());
            tmp_val_refv[pos_val].shrink_to_fit();
            pos_val = (nrow + 1) * i2;
            str_v.erase(str_v.begin() + pos_val, 
                            str_v.begin() + pos_val + delta_col);
          };
          str_v.shrink_to_fit();
          for (i2 = 0; i2 < matr_idx[1].size(); i2 += 1) {
            pos_val = matr_idx[1][i2];
            tmp_val_refv[pos_val].erase(tmp_val_refv[pos_val].begin() + nrow, 
                            tmp_val_refv[pos_val].end());
            tmp_val_refv[pos_val].shrink_to_fit();
            pos_val = (nrow + 1) * i2;
            chr_v.erase(chr_v.begin() + pos_val, 
                            chr_v.begin() + pos_val + delta_col);
          };
          chr_v.shrink_to_fit();
          for (i2 = 0; i2 < matr_idx[2].size(); i2 += 1) {
            pos_val = matr_idx[2][i2];
            tmp_val_refv[pos_val].erase(tmp_val_refv[pos_val].begin() + nrow, 
                            tmp_val_refv[pos_val].end());
            tmp_val_refv[pos_val].shrink_to_fit();
            pos_val = (nrow + 1) * i2;
            bool_v.erase(bool_v.begin() + pos_val, 
                            bool_v.begin() + pos_val + delta_col);
          };
          bool_v.shrink_to_fit();
          for (i2 = 0; i2 < matr_idx[3].size(); i2 += 1) {
            pos_val = matr_idx[3][i2];
            tmp_val_refv[pos_val].erase(tmp_val_refv[pos_val].begin() + nrow, 
                            tmp_val_refv[pos_val].end());
            tmp_val_refv[pos_val].shrink_to_fit();
            pos_val = (nrow + 1) * i2;
            int_v.erase(int_v.begin() + pos_val, 
                            int_v.begin() + pos_val + delta_col);
          };
          int_v.shrink_to_fit();
          for (i2 = 0; i2 < matr_idx[4].size(); i2 += 1) {
            pos_val = matr_idx[4][i2];
            tmp_val_refv[pos_val].erase(tmp_val_refv[pos_val].begin() + nrow, 
                            tmp_val_refv[pos_val].end());
            tmp_val_refv[pos_val].shrink_to_fit();
            pos_val = (nrow + 1) * i2;
            uint_v.erase(uint_v.begin() + pos_val, 
                            uint_v.begin() + pos_val + delta_col);
          };
          uint_v.shrink_to_fit();
          for (i2 = 0; i2 < matr_idx[5].size(); i2 += 1) {
            pos_val = matr_idx[5][i2];
            tmp_val_refv[pos_val].erase(tmp_val_refv[pos_val].begin() + nrow, 
                            tmp_val_refv[pos_val].end());
            tmp_val_refv[pos_val].shrink_to_fit();
            pos_val = (nrow + 1) * i2;
            dbl_v.erase(dbl_v.begin() + pos_val, 
                            dbl_v.begin() + pos_val + delta_col);
          };
          dbl_v.shrink_to_fit();
        };
    
        void merge_excluding_both(Dataframe &obj1, 
                                  Dataframe &obj2, 
                                  bool colname, 
                                  unsigned int &key1, 
                                  unsigned int &key2,
                                  std::string default_str = "NA",
                                  std::string default_chr = " ",
                                  std::string default_bool = "0",
                                  std::string default_int = "0",
                                  std::string default_uint = "0",
                                  std::string default_dbl = "0") {
          const unsigned int& ncol1 = obj1.get_ncol();
          const unsigned int& ncol2 = obj2.get_ncol();
          std::vector<std::string> cur_vstr;
          ncol = ncol1 + ncol2;
          unsigned int i;
          unsigned int i2;
          unsigned int i3;
          const std::vector<std::vector<unsigned int>>& matr_idx1 = obj1.get_matr_idx();
          const std::vector<std::vector<unsigned int>>& matr_idx2 = obj2.get_matr_idx();
          const std::vector<std::string>& name1 = obj1.get_colname();
          const std::vector<std::string>& name2 = obj2.get_colname();
          if (colname) {
            name_v.resize(ncol);
            if (name1.size() > 0) {
              for (i = 0; i < name1.size(); ++i) {
                name_v.push_back(name1[i]);
              };
            };
            if (name2.size() > 0) {
              for (i = 0; i < name2.size(); ++i) {
                name_v.push_back(name2[i]);
              };
            };
          };
          tmp_val_refv.reserve(ncol);
          for (i = 0; i < ncol; ++i) {
            tmp_val_refv.push_back(cur_vstr);
          };
          const std::vector<char>& type1 = obj1.get_typecol();
          const std::vector<char>& type2 = obj2.get_typecol();
          const auto& tmp1 = obj1.get_tmp_val_refv();
          const auto& tmp2 = obj2.get_tmp_val_refv();
          const std::vector<std::string>& col1 = tmp1[key1];
          const std::vector<std::string>& col2 = tmp2[key2];
          std::unordered_multimap<std::string, size_t> b_index;
          for (size_t j = 0; j < col2.size(); ++j) {
            b_index.insert({col2[j], j});
          };
          std::unordered_multimap<std::string, size_t> a_index;
          for (size_t j = 0; j < col1.size(); ++j) {
            a_index.insert({col1[j], j});
          };
          for (size_t i = 0; i < col1.size(); ++i) {
            auto range = b_index.equal_range(col1[i]);
            if (range.first == range.second) {
              nrow += 1;
              for (i2 = 0; i2 < ncol1; i2 += 1) {
                tmp_val_refv[i2].push_back(tmp1[i2][i]);
              };
              for (auto& el : matr_idx2[0]) {
                tmp_val_refv[ncol1 + el].push_back(default_str);
              };
              for (auto& el : matr_idx2[1]) {
                tmp_val_refv[ncol1 + el].push_back(default_chr);
              };
              for (auto& el : matr_idx2[2]) {
                tmp_val_refv[ncol1 + el].push_back(default_bool);
              };
              for (auto& el : matr_idx2[3]) {
                tmp_val_refv[ncol1 + el].push_back(default_int);
              };
              for (auto& el : matr_idx2[4]) {
                tmp_val_refv[ncol1 + el].push_back(default_uint);
              };
              for (auto& el : matr_idx2[5]) {
                tmp_val_refv[ncol1 + el].push_back(default_dbl);
              };
            };
          };
          for (size_t i = 0; i < col2.size(); ++i) {
            auto range = a_index.equal_range(col2[i]);
            if (range.first == range.second) {
              nrow += 1;
              for (auto& el : matr_idx1[0]) {
                tmp_val_refv[el].push_back(default_str);
              };
              for (auto& el : matr_idx1[1]) {
                tmp_val_refv[el].push_back(default_chr);
              };
              for (auto& el : matr_idx1[2]) {
                tmp_val_refv[el].push_back(default_bool);
              };
              for (auto& el : matr_idx1[3]) {
                tmp_val_refv[el].push_back(default_int);
              };
              for (auto& el : matr_idx1[4]) {
                tmp_val_refv[el].push_back(default_uint);
              };
              for (auto& el : matr_idx1[5]) {
                tmp_val_refv[el].push_back(default_dbl);
              };
              for (i2 = 0; i2 < ncol2; i2 += 1) {
                tmp_val_refv[ncol1 + i2].push_back(tmp2[i2][i]);
              };
            };
          };
          type_classification();
        };
    
        void merge_all(Dataframe &obj1, 
                        Dataframe &obj2, 
                        bool colname, 
                        unsigned int &key1, 
                        unsigned int &key2,
                        std::string default_str = "NA",
                        std::string default_chr = " ",
                        std::string default_bool = "0",
                        std::string default_int = "0",
                        std::string default_uint = "0",
                        std::string default_dbl = "0") {
          const unsigned int& ncol1 = obj1.get_ncol();
          const unsigned int& ncol2 = obj2.get_ncol();
          std::vector<std::string> cur_vstr;
          ncol = ncol1 + ncol2;
          unsigned int i;
          unsigned int i2;
          unsigned int i3;
          const std::vector<std::vector<unsigned int>>& matr_idx1 = obj1.get_matr_idx();
          const std::vector<std::vector<unsigned int>>& matr_idx2 = obj2.get_matr_idx();
          const std::vector<std::string>& name1 = obj1.get_colname();
          const std::vector<std::string>& name2 = obj2.get_colname();
          if (colname) {
            name_v.resize(ncol);
            if (name1.size() > 0) {
              for (i = 0; i < name1.size(); ++i) {
                name_v.push_back(name1[i]);
              };
            };
            if (name2.size() > 0) {
              for (i = 0; i < name2.size(); ++i) {
                name_v.push_back(name2[i]);
              };
            };
          };
          tmp_val_refv.reserve(ncol);
          for (i = 0; i < ncol; ++i) {
            tmp_val_refv.push_back(cur_vstr);
          };
          const std::vector<char>& type1 = obj1.get_typecol();
          const std::vector<char>& type2 = obj2.get_typecol();
          const auto& tmp1 = obj1.get_tmp_val_refv();
          const auto& tmp2 = obj2.get_tmp_val_refv();
          const std::vector<std::string>& col1 = tmp1[key1];
          const std::vector<std::string>& col2 = tmp2[key2];
          std::unordered_multimap<std::string, size_t> b_index;
          for (size_t j = 0; j < col2.size(); ++j) {
            b_index.insert({col2[j], j});
          };
          std::unordered_multimap<std::string, size_t> a_index;
          for (size_t j = 0; j < col1.size(); ++j) {
            a_index.insert({col1[j], j});
          };
          for (size_t i = 0; i < col1.size(); ++i) {
            auto range = b_index.equal_range(col1[i]);
            if (range.first == range.second) {
              nrow += 1;
              for (i2 = 0; i2 < ncol1; i2 += 1) {
                tmp_val_refv[i2].push_back(tmp1[i2][i]);
              };
              for (auto& el : matr_idx2[0]) {
                tmp_val_refv[ncol1 + el].push_back(default_str);
              };
              for (auto& el : matr_idx2[1]) {
                tmp_val_refv[ncol1 + el].push_back(default_chr);
              };
              for (auto& el : matr_idx2[2]) {
                tmp_val_refv[ncol1 + el].push_back(default_bool);
              };
              for (auto& el : matr_idx2[3]) {
                tmp_val_refv[ncol1 + el].push_back(default_int);
              };
              for (auto& el : matr_idx2[4]) {
                tmp_val_refv[ncol1 + el].push_back(default_uint);
              };
              for (auto& el : matr_idx2[5]) {
                tmp_val_refv[ncol1 + el].push_back(default_dbl);
              };
            } else {
              for (auto it = range.first; it != range.second; ++it) {
                nrow += 1;
                size_t idx = it->second;
                for (i2 = 0; i2 < ncol1; i2 += 1) {
                  tmp_val_refv[i2].push_back(tmp1[i2][i]);
                };
                for (i2 = 0; i2 < ncol2; i2 += 1) {
                  tmp_val_refv[ncol1 + i2].push_back(tmp2[i2][idx]);
                };
              };
            };
          };
          for (size_t i = 0; i < col2.size(); ++i) {
            auto range = a_index.equal_range(col2[i]);
            if (range.first == range.second) {
              nrow += 1;
              for (auto& el : matr_idx1[0]) {
                tmp_val_refv[el].push_back(default_str);
              };
              for (auto& el : matr_idx1[1]) {
                tmp_val_refv[el].push_back(default_chr);
              };
              for (auto& el : matr_idx1[2]) {
                tmp_val_refv[el].push_back(default_bool);
              };
              for (auto& el : matr_idx1[3]) {
                tmp_val_refv[el].push_back(default_int);
              };
              for (auto& el : matr_idx1[4]) {
                tmp_val_refv[el].push_back(default_uint);
              };
              for (auto& el : matr_idx1[5]) {
                tmp_val_refv[el].push_back(default_dbl);
              };
              for (i2 = 0; i2 < ncol2; i2 += 1) {
                tmp_val_refv[ncol1 + i2].push_back(tmp2[i2][i]);
              };
            };
          };
          type_classification();
        };
    
        void merge_all2(Dataframe &obj1, 
                        Dataframe &obj2, 
                        bool colname, 
                        unsigned int &key1, 
                        unsigned int &key2,
                        std::string default_str = "NA",
                        std::string default_chr = " ",
                        std::string default_bool = "0",
                        std::string default_int = "0",
                        std::string default_uint = "0",
                        std::string default_dbl = "0") {
          const unsigned int& ncol1 = obj1.get_ncol();
          const unsigned int& ncol2 = obj2.get_ncol();
          std::vector<std::string> cur_vstr;
          ncol = ncol1 + ncol2;
          unsigned int i;
          unsigned int i2;
          unsigned int i3;
          const std::vector<std::vector<unsigned int>>& matr_idx1 = obj1.get_matr_idx();
          const std::vector<std::vector<unsigned int>>& matr_idx2 = obj2.get_matr_idx();
          const std::vector<std::string>& name1 = obj1.get_colname();
          const std::vector<std::string>& name2 = obj2.get_colname();
          if (colname) {
            name_v.resize(ncol);
            if (name1.size() > 0) {
              for (i = 0; i < name1.size(); ++i) {
                name_v.push_back(name1[i]);
              };
            };
            if (name2.size() > 0) {
              for (i = 0; i < name2.size(); ++i) {
                name_v.push_back(name2[i]);
              };
            };
          };
          tmp_val_refv.reserve(ncol);
          for (i = 0; i < ncol; ++i) {
            tmp_val_refv.push_back(cur_vstr);
          };
          const std::vector<char>& type1 = obj1.get_typecol();
          const std::vector<char>& type2 = obj2.get_typecol();
          const auto& tmp1 = obj1.get_tmp_val_refv();
          const auto& tmp2 = obj2.get_tmp_val_refv();
          const std::vector<std::string>& col1 = tmp1[key1];
          const std::vector<std::string>& col2 = tmp2[key2];
          std::unordered_multimap<std::string, size_t> b_index;
          for (size_t j = 0; j < col2.size(); ++j) {
            b_index.insert({col2[j], j});
          };
          std::unordered_multimap<std::string, size_t> a_index;
          for (size_t j = 0; j < col1.size(); ++j) {
            a_index.insert({col1[j], j});
          };
          for (size_t i = 0; i < col1.size(); ++i) {
            auto it = b_index.find(col1[i]);
            if (it == b_index.end()) {
              nrow += 1;
              for (i2 = 0; i2 < ncol1; i2 += 1) {
                tmp_val_refv[i2].push_back(tmp1[i2][i]);
              };
              for (auto& el : matr_idx2[0]) {
                tmp_val_refv[ncol1 + el].push_back(default_str);
              };
              for (auto& el : matr_idx2[1]) {
                tmp_val_refv[ncol1 + el].push_back(default_chr);
              };
              for (auto& el : matr_idx2[2]) {
                tmp_val_refv[ncol1 + el].push_back(default_bool);
              };
              for (auto& el : matr_idx2[3]) {
                tmp_val_refv[ncol1 + el].push_back(default_int);
              };
              for (auto& el : matr_idx2[4]) {
                tmp_val_refv[ncol1 + el].push_back(default_uint);
              };
              for (auto& el : matr_idx2[5]) {
                tmp_val_refv[ncol1 + el].push_back(default_dbl);
              };
            } else {
              nrow += 1;
              size_t idx = it->second;
              for (i2 = 0; i2 < ncol1; i2 += 1) {
                tmp_val_refv[i2].push_back(tmp1[i2][i]);
              };
              for (i2 = 0; i2 < ncol2; i2 += 1) {
                tmp_val_refv[ncol1 + i2].push_back(tmp2[i2][idx]);
              };
            };
          };
          for (size_t i = 0; i < col2.size(); ++i) {
            auto it = a_index.find(col2[i]);
            if (it == a_index.end()) {
              nrow += 1;
              for (i2 = 0; i2 < ncol2; i2 += 1) {
                tmp_val_refv[ncol1 + i2].push_back(tmp2[i2][i]);
              };
              for (auto& el : matr_idx1[0]) {
                tmp_val_refv[el].push_back(default_str);
              };
              for (auto& el : matr_idx1[1]) {
                tmp_val_refv[el].push_back(default_chr);
              };
              for (auto& el : matr_idx1[2]) {
                tmp_val_refv[el].push_back(default_bool);
              };
              for (auto& el : matr_idx1[3]) {
                tmp_val_refv[el].push_back(default_int);
              };
              for (auto& el : matr_idx1[4]) {
                tmp_val_refv[el].push_back(default_uint);
              };
              for (auto& el : matr_idx1[5]) {
                tmp_val_refv[el].push_back(default_dbl);
              };
              for (i2 = 0; i2 < ncol2; i2 += 1) {
                tmp_val_refv[i2].push_back(tmp2[i2][i]);
              };
            };
          };
          type_classification();
        };
    
        void transform_left_join(Dataframe &obj, 
                        unsigned int &key1, 
                        unsigned int &key2,
                        std::string default_str = "NA",
                        char default_chr = ' ',
                        bool default_bool = 0,
                        int default_int = 0,
                        unsigned int default_uint = 0,
                        double default_dbl = 0) {
          const unsigned int& ncol2 = obj.get_ncol();
          const std::vector<std::vector<unsigned int>>& matr_idx2 = obj.get_matr_idx();
    
          const std::vector<std::string>& str_v2 = obj.get_str_vec();
          const std::vector<char>& chr_v2 = obj.get_chr_vec();
          const std::vector<bool>& bool_v2 = obj.get_bool_vec();
          const std::vector<int>& int_v2 = obj.get_int_vec();
          const std::vector<unsigned int>& uint_v2 = obj.get_uint_vec();
          const std::vector<double>& dbl_v2 = obj.get_dbl_vec();
          
          const unsigned int size_str = str_v.size() / nrow;
          const unsigned int size_chr = chr_v.size() / nrow;
          const unsigned int size_bool = bool_v.size() / nrow;
          const unsigned int size_int = int_v.size() / nrow;
          const unsigned int size_uint = uint_v.size() / nrow;
          const unsigned int size_dbl = dbl_v.size() / nrow;
          
          unsigned int i;
          unsigned int i2;
          
          std::vector<std::string> vec_str(nrow);
    
          for (i = 0; i < ncol2; i += 1) {
            tmp_val_refv.push_back(vec_str);
          };
    
          const std::vector<char>& vec_type = obj.get_typecol();
          const std::vector<std::string>& col1 = tmp_val_refv[key1];
          const std::vector<std::vector<std::string>>& tmp_val_refv2 = obj.get_tmp_val_refv();
          const std::vector<std::string>& col2 = tmp_val_refv2[key2];
    
          str_v.resize(str_v.size() + nrow * matr_idx2[0].size());
          chr_v.resize(chr_v.size() + nrow * matr_idx2[1].size());
          bool_v.resize(bool_v.size() + nrow * matr_idx2[2].size());
          int_v.resize(int_v.size() + nrow * matr_idx2[3].size());
          uint_v.resize(uint_v.size() + nrow * matr_idx2[4].size());
          dbl_v.resize(dbl_v.size() + nrow * matr_idx2[5].size());
         
          std::vector<std::vector<unsigned int>> matr_idx2b = matr_idx2;
          for (auto& el : matr_idx2b) {
            for (auto& el2 : el) {
              el2 += ncol;
            };
          };
    
          matr_idx[0].insert(matr_idx[0].end(), 
                             matr_idx2b[0].begin(), 
                             matr_idx2b[0].end());
          matr_idx[1].insert(matr_idx[1].end(), 
                             matr_idx2b[1].begin(), 
                             matr_idx2b[1].end());
          matr_idx[2].insert(matr_idx[2].end(), 
                             matr_idx2b[2].begin(), 
                             matr_idx2b[2].end());
          matr_idx[3].insert(matr_idx[3].end(), 
                             matr_idx2b[3].begin(), 
                             matr_idx2b[3].end());
          matr_idx[4].insert(matr_idx[4].end(), 
                             matr_idx2b[4].begin(), 
                             matr_idx2b[4].end());
          matr_idx[5].insert(matr_idx[5].end(), 
                             matr_idx2b[5].begin(), 
                             matr_idx2b[5].end());
    
    
          std::unordered_multimap<std::string, size_t> lookup;
          for (i = 0; i < col2.size(); i += 1) {
            lookup.insert({col2[i], i});
          };
     
          unsigned int idx;
          unsigned int pos_vl;
          const unsigned int& nrow2 = obj.get_nrow();
          for (i = 0; i < col1.size(); i += 1) {
    
            auto it = lookup.find(col1[i]);
            if (it == lookup.end()) {
              for (i2 = 0; i2 < matr_idx2[0].size(); i2 += 1) {
                tmp_val_refv[ncol + matr_idx2[0][i2]][i] = default_str;
                str_v[nrow * (size_str + i2) + i] = default_str;
              };
              for (i2 = 0; i2 < matr_idx2[1].size(); i2 += 1) {
                tmp_val_refv[ncol + matr_idx2[1][i2]][i] = std::string(1, default_chr);
                chr_v[nrow * (size_chr + i2) + i] = default_chr;
              };
              for (i2 = 0; i2 < matr_idx2[2].size(); i2 += 1) {
                tmp_val_refv[ncol + matr_idx2[2][i2]][i] = std::to_string(default_bool);
                bool_v[nrow * (size_bool + i2) + i] = default_bool;
              };
              for (i2 = 0; i2 < matr_idx2[3].size(); i2 += 1) {
                tmp_val_refv[ncol + matr_idx2[3][i2]][i] = std::to_string(default_int);
                int_v[nrow * (size_int + i2) + i] = default_int;
              };
              for (i2 = 0; i2 < matr_idx2[4].size(); i2 += 1) {
                tmp_val_refv[ncol + matr_idx2[4][i2]][i] = std::to_string(default_uint);
                uint_v[nrow * (size_uint + i2) + i] = default_uint;
              };
              for (i2 = 0; i2 < matr_idx2[5].size(); i2 += 1) {
                tmp_val_refv[ncol + matr_idx2[5][i2]][i] = std::to_string(default_dbl);
                dbl_v[nrow * (size_dbl + i2) + i] = default_dbl;
              };
            } else {
              idx = it->second;
              for (i2 = 0; i2 < matr_idx2[0].size(); i2 += 1) {
                str_v[nrow * (size_str + i2) + i] = str_v2[nrow2 * i2 + idx];
                pos_vl = matr_idx2[0][i2];
                tmp_val_refv[ncol + pos_vl][i] = tmp_val_refv2[pos_vl][idx];
              };
              for (i2 = 0; i2 < matr_idx2[1].size(); i2 += 1) {
                chr_v[nrow * (size_chr + i2) + i] = chr_v2[nrow2 * i2 + idx];
                pos_vl = matr_idx2[1][i2];
                tmp_val_refv[ncol + pos_vl][i] = tmp_val_refv2[pos_vl][idx];
              };
              for (i2 = 0; i2 < matr_idx2[2].size(); i2 += 1) {
                bool_v[nrow * (size_bool + i2) + i] = bool_v2[nrow2 * i2 + idx];
                pos_vl = matr_idx2[2][i2];
                tmp_val_refv[ncol + pos_vl][i] = tmp_val_refv2[pos_vl][idx];
              };
              for (i2 = 0; i2 < matr_idx2[3].size(); i2 += 1) {
                int_v[nrow * (size_int + i2) + i] = int_v2[nrow2 * i2 + idx];
                pos_vl = matr_idx2[3][i2];
                tmp_val_refv[ncol + pos_vl][i] = tmp_val_refv2[pos_vl][idx];
              };
              for (i2 = 0; i2 < matr_idx2[4].size(); i2 += 1) {
                uint_v[nrow * (size_uint + i2) + i] = uint_v2[nrow2 * i2 + idx];
                pos_vl = matr_idx2[4][i2];
                tmp_val_refv[ncol + pos_vl][i] = tmp_val_refv2[pos_vl][idx];
              };
              for (i2 = 0; i2 < matr_idx2[5].size(); i2 += 1) {
                dbl_v[nrow * (size_dbl + i2) + i] = dbl_v2[nrow2 * i2 + idx];
                pos_vl = matr_idx2[5][i2];
                tmp_val_refv[ncol + pos_vl][i] = tmp_val_refv2[pos_vl][idx];
              };
            };
          };
          type_refv.insert(type_refv.end(), vec_type.begin(), vec_type.end());
          ncol += ncol2;
          const std::vector<std::string>& colname2 = obj.get_colname();
          if (colname2.size() > 0) {
            name_v.insert(name_v.end(), colname2.begin(), colname2.end());
          } else {
            name_v.resize(ncol);
          };
        };
    
        void transform_filter(std::vector<bool>& mask) {
          unsigned int i2;
          unsigned int nrow2 = nrow;
          nrow = 0;
          unsigned int pos_vl;
          for (unsigned int i = 0; i < nrow2; i += 1) {
            if (mask[i]) {
              for (i2 = 0 ; i2 < matr_idx[0].size(); i2 += 1) {
                pos_vl = matr_idx[0][i2];
                tmp_val_refv[pos_vl][nrow] = tmp_val_refv[pos_vl][i];
                str_v[nrow2 * i2 + nrow] = str_v[nrow2 * i2 + i];
              };
              for (i2 = 0 ; i2 < matr_idx[1].size(); i2 += 1) {
                pos_vl = matr_idx[1][i2];
                tmp_val_refv[pos_vl][nrow] = tmp_val_refv[pos_vl][i];
                chr_v[nrow2 * i2 + nrow] = chr_v[nrow2 * i2 + i];
              };
              for (i2 = 0 ; i2 < matr_idx[2].size(); i2 += 1) {
                pos_vl = matr_idx[2][i2];
                tmp_val_refv[pos_vl][nrow] = tmp_val_refv[pos_vl][i];
                bool_v[nrow2 * i2 + nrow] = bool_v[nrow2 * i2 + i];
              };
              for (i2 = 0 ; i2 < matr_idx[3].size(); i2 += 1) {
                pos_vl = matr_idx[3][i2];
                tmp_val_refv[pos_vl][nrow] = tmp_val_refv[pos_vl][i];
                int_v[nrow2 * i2 + nrow] = int_v[nrow2 * i2 + i];
              };
              for (i2 = 0 ; i2 < matr_idx[4].size(); i2 += 1) {
                pos_vl = matr_idx[4][i2];
                tmp_val_refv[pos_vl][nrow] = tmp_val_refv[pos_vl][i];
                uint_v[nrow2 * i2 + nrow] = uint_v[nrow2 * i2 + i];
              };
              for (i2 = 0 ; i2 < matr_idx[5].size(); i2 += 1) {
                pos_vl = matr_idx[5][i2];
                tmp_val_refv[pos_vl][nrow] = tmp_val_refv[pos_vl][i];
                dbl_v[nrow2 * i2 + nrow] = dbl_v[nrow2 * i2 + i];
              };
              nrow += 1;
            };
          }
        };
    
        void transform_unique(unsigned int& n) {
          unsigned int nrow2 = nrow;
          unsigned int i;
          unsigned int i2;
          unsigned int pos_vl;
          nrow = 0;
          //std::unordered_set<std::string> unic_v; // standard set (slower)
          ankerl::unordered_dense::set<std::string> unic_v;
    
          for (i = 0; i < nrow2; i += 1) {
            if (!unic_v.contains(tmp_val_refv[n][i])) {
              unic_v.insert(tmp_val_refv[n][i]);
              for (i2 = 0; i2 < matr_idx[0].size(); i2 += 1) {
                pos_vl = matr_idx[0][i2];
                tmp_val_refv[pos_vl][nrow] = tmp_val_refv[pos_vl][i];
                str_v[nrow2 * i2 + nrow] = str_v[nrow2 * i2 + i];
              };
              for (i2 = 0; i2 < matr_idx[1].size(); i2 += 1) {
                pos_vl = matr_idx[1][i2];
                tmp_val_refv[pos_vl][nrow] = tmp_val_refv[pos_vl][i];
                chr_v[nrow2 * i2 + nrow] = chr_v[nrow2 * i2 + i];
              };
              for (i2 = 0; i2 < matr_idx[2].size(); i2 += 1) {
                pos_vl = matr_idx[2][i2];
                tmp_val_refv[pos_vl][nrow] = tmp_val_refv[pos_vl][i];
                bool_v[nrow2 * i2 + nrow] = bool_v[nrow2 * i2 + i];
              };
              for (i2 = 0; i2 < matr_idx[3].size(); i2 += 1) {
                pos_vl = matr_idx[3][i2];
                tmp_val_refv[pos_vl][nrow] = tmp_val_refv[pos_vl][i];
                int_v[nrow2 * i2 + nrow] = int_v[nrow2 * i2 + i];
              };
              for (i2 = 0; i2 < matr_idx[4].size(); i2 += 1) {
                pos_vl = matr_idx[4][i2];
                tmp_val_refv[pos_vl][nrow] = tmp_val_refv[pos_vl][i];
                uint_v[nrow2 * i2 + nrow] = uint_v[nrow2 * i2 + i];
              };
              for (i2 = 0; i2 < matr_idx[5].size(); i2 += 1) {
                pos_vl = matr_idx[5][i2];
                tmp_val_refv[pos_vl][nrow] = tmp_val_refv[pos_vl][i];
                dbl_v[nrow2 * i2 + nrow] = dbl_v[nrow2 * i2 + i];
              };
              nrow += 1;
            };
          };
        };
    
        void transform_unique_clean(unsigned int& n) {
          unsigned int nrow2 = nrow;
          unsigned int i;
          unsigned int i2;
          unsigned int pos_vl;
          nrow = 0;
          //std::unordered_set<std::string> unic_v; // standard set (slower)
          ankerl::unordered_dense::set<std::string> unic_v;
    
          for (i = 0; i < nrow2; i += 1) {
            if (!unic_v.contains(tmp_val_refv[n][i])) {
              unic_v.insert(tmp_val_refv[n][i]);
              for (i2 = 0; i2 < matr_idx[0].size(); i2 += 1) {
                pos_vl = matr_idx[0][i2];
                tmp_val_refv[pos_vl][nrow] = tmp_val_refv[pos_vl][i];
                str_v[nrow2 * i2 + nrow] = str_v[nrow2 * i2 + i];
              };
              for (i2 = 0; i2 < matr_idx[1].size(); i2 += 1) {
                pos_vl = matr_idx[1][i2];
                tmp_val_refv[pos_vl][nrow] = tmp_val_refv[pos_vl][i];
                chr_v[nrow2 * i2 + nrow] = chr_v[nrow2 * i2 + i];
              };
              for (i2 = 0; i2 < matr_idx[2].size(); i2 += 1) {
                pos_vl = matr_idx[2][i2];
                tmp_val_refv[pos_vl][nrow] = tmp_val_refv[pos_vl][i];
                bool_v[nrow2 * i2 + nrow] = bool_v[nrow2 * i2 + i];
              };
              for (i2 = 0; i2 < matr_idx[3].size(); i2 += 1) {
                pos_vl = matr_idx[3][i2];
                tmp_val_refv[pos_vl][nrow] = tmp_val_refv[pos_vl][i];
                int_v[nrow2 * i2 + nrow] = int_v[nrow2 * i2 + i];
              };
              for (i2 = 0; i2 < matr_idx[4].size(); i2 += 1) {
                pos_vl = matr_idx[4][i2];
                tmp_val_refv[pos_vl][nrow] = tmp_val_refv[pos_vl][i];
                uint_v[nrow2 * i2 + nrow] = uint_v[nrow2 * i2 + i];
              };
              for (i2 = 0; i2 < matr_idx[5].size(); i2 += 1) {
                pos_vl = matr_idx[5][i2];
                tmp_val_refv[pos_vl][nrow] = tmp_val_refv[pos_vl][i];
                dbl_v[nrow2 * i2 + nrow] = dbl_v[nrow2 * i2 + i];
              };
              nrow += 1;
            };
          };
         
          unsigned int pos_vl2;
          unsigned int delta_col = nrow2 - nrow;
          for (i2 = 0; i2 < matr_idx[0].size(); i2 += 1) {
            pos_vl = matr_idx[0][i2];
            pos_vl2 = (nrow + 1) * i2;
            str_v.erase(str_v.begin() + pos_vl2, 
                            str_v.begin() + pos_vl2 + delta_col);
            tmp_val_refv[pos_vl].erase(tmp_val_refv[pos_vl].begin() + nrow, 
                            tmp_val_refv[pos_vl].end());
            tmp_val_refv[pos_vl].shrink_to_fit();
          };
          str_v.shrink_to_fit();
          for (i2 = 0; i2 < matr_idx[1].size(); i2 += 1) {
            pos_vl = matr_idx[1][i2];
            pos_vl2 = (nrow + 1) * i2;
            chr_v.erase(chr_v.begin() + pos_vl2, 
                            chr_v.begin() + pos_vl2 + delta_col);
            tmp_val_refv[pos_vl].erase(tmp_val_refv[pos_vl].begin() + nrow, 
                            tmp_val_refv[pos_vl].end());
            tmp_val_refv[pos_vl].shrink_to_fit();
          };
          chr_v.shrink_to_fit();
          for (i2 = 0; i2 < matr_idx[2].size(); i2 += 1) {
            pos_vl = matr_idx[2][i2];
            pos_vl2 = (nrow + 1) * i2;
            bool_v.erase(bool_v.begin() + pos_vl2, 
                            bool_v.begin() + pos_vl2 + delta_col);
            tmp_val_refv[pos_vl].erase(tmp_val_refv[pos_vl].begin() + nrow, 
                            tmp_val_refv[pos_vl].end());
            tmp_val_refv[pos_vl].shrink_to_fit();
          };
          bool_v.shrink_to_fit();
          for (i2 = 0; i2 < matr_idx[3].size(); i2 += 1) {
            pos_vl = matr_idx[3][i2];
            pos_vl2 = (nrow + 1) * i2;
            int_v.erase(int_v.begin() + pos_vl2, 
                            int_v.begin() + pos_vl2 + delta_col);
            tmp_val_refv[pos_vl].erase(tmp_val_refv[pos_vl].begin() + nrow, 
                            tmp_val_refv[pos_vl].end());
            tmp_val_refv[pos_vl].shrink_to_fit();
          };
          int_v.shrink_to_fit();
          for (i2 = 0; i2 < matr_idx[4].size(); i2 += 1) {
            pos_vl = matr_idx[4][i2];
            pos_vl2 = (nrow + 1) * i2;
            uint_v.erase(uint_v.begin() + pos_vl2, 
                            uint_v.begin() + pos_vl2 + delta_col);
            tmp_val_refv[pos_vl].erase(tmp_val_refv[pos_vl].begin() + nrow, 
                            tmp_val_refv[pos_vl].end());
            tmp_val_refv[pos_vl].shrink_to_fit();
          };
          uint_v.shrink_to_fit();
          for (i2 = 0; i2 < matr_idx[5].size(); i2 += 1) {
            pos_vl = matr_idx[5][i2];
            pos_vl2 = (nrow + 1) * i2;
            dbl_v.erase(dbl_v.begin() + pos_vl2, 
                            dbl_v.begin() + pos_vl2 + delta_col);
            tmp_val_refv[pos_vl].erase(tmp_val_refv[pos_vl].begin() + nrow, 
                            tmp_val_refv[pos_vl].end());
            tmp_val_refv[pos_vl].shrink_to_fit();
          };
          dbl_v.shrink_to_fit();
        };
    
        void transform_group_by(std::vector<unsigned int> &x,
                                std::string sumcolname = "n") {
          //std::unordered_map<std::string, unsigned int> lookup; // standard map (slower)
          ankerl::unordered_dense::map<std::string, unsigned int> lookup;
          std::vector<unsigned int> occ_v;
          std::vector<std::string> occ_v_str;
          occ_v_str.reserve(nrow);
          occ_v.reserve(nrow);
          std::string key;
          std::vector<std::string> key_vec(nrow);
          unsigned int i;
          unsigned int i2;
          for (i = 0; i < nrow; i += 1) {
            key = tmp_val_refv[x[0]][i];
            for (i2 = 1; i2 < x.size(); i2 += 1) {
              key += tmp_val_refv[x[i2]][i];
            };
            lookup[key] += 1;
            key_vec[i] = key;
          };
          unsigned int occ_val;
          for (auto& el : key_vec) {
            occ_val = lookup[el];
            occ_v.push_back(occ_val);
            occ_v_str.push_back(std::to_string(occ_val));
          };
          uint_v.insert(uint_v.end(), occ_v.begin(), occ_v.end());
          tmp_val_refv.push_back(occ_v_str);
          if (name_v.size() > 0) {
            name_v.push_back(sumcolname);
          };
          type_refv.push_back('u');
          ncol += 1;
        };
    
        void pivot_int(Dataframe &obj, unsigned int &n1, unsigned int& n2, unsigned int& n3) {
          const std::vector<std::vector<std::string>>& tmp = obj.get_tmp_val_refv();
          const std::vector<std::string>& col_vec = tmp[n1];
          const std::vector<std::string>& row_vec = tmp[n2];
          const unsigned int& nrow2 = obj.get_nrow();
          const std::vector<std::vector<unsigned int>>& matr_idx2 = obj.get_matr_idx();
          unsigned int i = 0;
          unsigned int pos_val;
          const std::vector<int>& cur_int_v = obj.get_int_vec();
    
          std::vector<int> tmp_int_v = {};
    
          //std::unordered_map<std::pair<std::string_view, std::string_view>, int, PairHash> lookup; // standard map (slower)
          ankerl::unordered_dense::map<std::pair<std::string_view, std::string_view>, int, PairHash> lookup;
    
          std::string key;
          for (auto& el : matr_idx2[3]) {
            if (n3 == el) {
              pos_val = nrow2 * i;
              tmp_int_v.insert(tmp_int_v.end(), 
                              cur_int_v.begin() + pos_val, 
                              cur_int_v.begin() + pos_val + nrow2);
            };
            i += 1;
          };
          //std::unordered_map<std::string, int> idx_col; // standard map (slower)
          ankerl::unordered_dense::map<std::string, int> idx_col;
          //std::unordered_map<std::string, int> idx_row;
          ankerl::unordered_dense::map<std::string, int> idx_row;
    
          for (i = 0; i < nrow2; i += 1) {
            key = col_vec[i];
            if (!idx_col.contains(key)) {
              idx_col[key] = idx_col.size();
            };
            key = row_vec[i];
            if (!idx_row.contains(key)) {
              idx_row[key] = idx_row.size();
            };
            lookup[{col_vec[i], row_vec[i]}] += tmp_int_v[i];
          };
          ncol = idx_row.size();
          nrow = idx_col.size();
          int_v.resize(ncol * nrow);
    
          std::vector<std::string> cur_vec_str(idx_row.size());
          tmp_val_refv.resize(ncol, cur_vec_str);
         
          int cur_int;      
          for (const auto& [key_v, value] : idx_col) {
            for (const auto& [key_v2, value2] : idx_row) {
              auto key_pair = std::pair<std::string_view, std::string_view> {key_v, key_v2};
              if (lookup.contains(key_pair)) {
                cur_int = lookup[key_pair];
                int_v[value * nrow + value2] = cur_int;
                tmp_val_refv[value][value2] = std::to_string(cur_int);
              };
            };
          };
          name_v.resize(idx_col.size());
          i = 0;
          matr_idx[3].resize(ncol);
          for (auto& [key_v, value] : idx_col) {
            type_refv.push_back('i');
            name_v[value] = key_v;
            matr_idx[3][i] = i;
            i += 1;
          };
          name_v_row.resize(idx_row.size());
          for (auto& [key_v, value] : idx_row) {
            name_v_row[value] = key_v;
          };
        };
    
        void pivot_uint(Dataframe &obj, unsigned int &n1, unsigned int& n2, unsigned int& n3) {
          const std::vector<std::vector<std::string>>& tmp = obj.get_tmp_val_refv();
          const std::vector<std::string>& col_vec = tmp[n1];
          const std::vector<std::string>& row_vec = tmp[n2];
          const unsigned int& nrow2 = obj.get_nrow();
          const std::vector<std::vector<unsigned int>>& matr_idx2 = obj.get_matr_idx();
          unsigned int i = 0;
          unsigned int pos_val;
          const std::vector<unsigned int>& cur_uint_v = obj.get_uint_vec();
    
          std::vector<unsigned int> tmp_uint_v = {};
          //std::unordered_map<std::pair<std::string_view, std::string_view>, unsigned int, PairHash> lookup; // standard map (slower)
          ankerl::unordered_dense::map<std::pair<std::string_view, std::string_view>, unsigned int, PairHash> lookup; 
    
          std::string key;
          for (auto& el : matr_idx2[4]) {
            if (n3 == el) {
              pos_val = nrow2 * i;
              tmp_uint_v.insert(tmp_uint_v.end(), 
                              cur_uint_v.begin() + pos_val, 
                              cur_uint_v.begin() + pos_val + nrow2);
            };
            i += 1;
          };
          //std::unordered_map<std::string, unsigned int> idx_col; //standard map (slower)
          ankerl::unordered_dense::map<std::string, unsigned int> idx_col;
          //std::unordered_map<std::string, unsigned int> idx_row; // standard map (slower)
          ankerl::unordered_dense::map<std::string, unsigned int> idx_row;
    
          for (i = 0; i < nrow2; i += 1) {
            key = col_vec[i];
            if (!idx_col.contains(key)) {
              idx_col[key] = idx_col.size();
            };
            key = row_vec[i];
            if (!idx_row.contains(key)) {
              idx_row[key] = idx_row.size();
            };
            lookup[{ col_vec[i], row_vec[i] }] += tmp_uint_v[i];
          };
          ncol = idx_row.size();
          nrow = idx_col.size();
          uint_v.resize(ncol * nrow);
    
          std::vector<std::string> cur_vec_str(idx_row.size());
          tmp_val_refv.resize(ncol, cur_vec_str);
         
          unsigned int cur_uint;      
          for (const auto& [key_v, value] : idx_col) {
            for (const auto& [key_v2, value2] : idx_row) {
              auto key_pair = std::pair<std::string_view, std::string_view>{key_v, key_v2};
              if (lookup.contains(key_pair)) {
                cur_uint = lookup[key_pair];
                uint_v[value * nrow + value2] = cur_uint;
                tmp_val_refv[value][value2] = std::to_string(cur_uint);
              };
            };
          };
          name_v.resize(idx_col.size());
          i = 0;
          matr_idx[4].resize(ncol);
          for (auto& [key_v, value] : idx_col) {
            type_refv.push_back('u');
            name_v[value] = key_v;
            matr_idx[4][i] = i;
            i += 1;
          };
          name_v_row.resize(idx_row.size());
          for (auto& [key_v, value] : idx_row) {
            name_v_row[value] = key_v;
          };
        };
    
        void pivot_dbl(Dataframe &obj, unsigned int &n1, unsigned int& n2, unsigned int& n3) {
          const std::vector<std::vector<std::string>>& tmp = obj.get_tmp_val_refv();
          const std::vector<std::string>& col_vec = tmp[n1];
          const std::vector<std::string>& row_vec = tmp[n2];
          const unsigned int& nrow2 = obj.get_nrow();
          const std::vector<std::vector<unsigned int>>& matr_idx2 = obj.get_matr_idx();
          unsigned int i = 0;
          unsigned int pos_val;
          const std::vector<double>& cur_dbl_v = obj.get_dbl_vec();
    
          std::vector<double> tmp_dbl_v = {};
    
          //std::unordered_map<std::pair<std::string_view, std::string_view>, double, PairHash> lookup; //standard map (slower)
          ankerl::unordered_dense::map<std::pair<std::string_view, std::string_view>, double, PairHash> lookup;
    
    
          std::string key;
          for (auto& el : matr_idx2[4]) {
            if (n3 == el) {
              pos_val = nrow2 * i;
              tmp_dbl_v.insert(tmp_dbl_v.end(), 
                              cur_dbl_v.begin() + pos_val, 
                              cur_dbl_v.begin() + pos_val + nrow2);
            };
            i += 1;
          };
          //std::unordered_map<std::string, unsigned int> idx_col; // standard map (slower)
          ankerl::unordered_dense::map<std::string, unsigned int> idx_col;
          //std::unordered_map<std::string, unsigned int> idx_row; // standard map (slower)
          ankerl::unordered_dense::map<std::string, unsigned int> idx_row;
    
          for (i = 0; i < nrow2; i += 1) {
            key = col_vec[i];
            if (!idx_col.contains(key)) {
              idx_col[key] = idx_col.size();
            };
            key = row_vec[i];
            if (!idx_row.contains(key)) {
              idx_row[key] = idx_row.size();
            };
            lookup[{col_vec[i], row_vec[i]}] += tmp_dbl_v[i];
          };
          ncol = idx_row.size();
          nrow = idx_col.size();
          dbl_v.resize(ncol * nrow);
    
          std::vector<std::string> cur_vec_str(idx_row.size());
          tmp_val_refv.resize(ncol, cur_vec_str);
         
          double cur_dbl;
          for (const auto& [key_v, value] : idx_col) {
            for (const auto& [key_v2, value2] : idx_row) {
              auto key_pair = std::pair<std::string_view, std::string_view>{key_v, key_v2};
              if (lookup.contains(key_pair)) {
                cur_dbl = lookup[key_pair];
                dbl_v[value * nrow + value2] = cur_dbl;
                tmp_val_refv[value][value2] = std::to_string(cur_dbl);
              };
            };
          };
          name_v.resize(idx_col.size());
          i = 0;
          matr_idx[4].resize(ncol);
          for (auto& [key_v, value] : idx_col) {
            type_refv.push_back('d');
            name_v[value] = key_v;
            matr_idx[4][i] = i;
            i += 1;
          };
          name_v_row.resize(idx_row.size());
          for (auto& [key_v, value] : idx_row) {
            name_v_row[value] = key_v;
          };
        };
    
        template <bool asc = 1>
        void sort_by(unsigned int& n) {
    
          std::vector<size_t> idx(nrow);
          std::iota(idx.begin(), idx.end(), 0);
          unsigned int i = 0;
          unsigned int i2;
          if constexpr (asc) {
            switch(type_refv[n]) {
              case 's': {
    
                          while (n != matr_idx[0][i]) {
                            i += 1;
                          }
                          if (i == matr_idx[0].size()) {
                            std::cerr << "Column not found.\n";
                            return;
                          };
                          const std::span<std::string> values(str_v.data() + i * nrow, nrow);
                          std::sort(idx.begin(), idx.end(),
                          [&](size_t a, size_t b) {
                              return values[a] < values[b];
                          });
    
                          break;
    
                        };
              case 'c': {
    
                          while (n != matr_idx[1][i]) {
                            i += 1;
                          }
                          if (i == matr_idx[1].size()) {
                            std::cerr << "Column not found.\n";
                            return;
                          };
                          const std::span<char> values(chr_v.data() + i * nrow, nrow);
                          std::sort(idx.begin(), idx.end(),
                          [&](size_t a, size_t b) {
                              return values[a] < values[b];
                          });
    
                          break;
    
                        };
              case 'b': {
    
                          while (n != matr_idx[2][i]) {
                            i += 1;
                          }
                          if (i == matr_idx[2].size()) {
                            std::cerr << "Column not found.\n";
                            return;
                          };
                          std::vector<bool> values(bool_v.begin() + i * nrow, 
                                          bool_v.begin() + (i + 1) * nrow);
                          std::sort(idx.begin(), idx.end(),
                          [&](size_t a, size_t b) {
                              return values[a] < values[b];
                          });
    
                          break;
    
                        };
              case 'i': {
    
                          while (n != matr_idx[3][i]) {
                            i += 1;
                          }
                          if (i == matr_idx[3].size()) {
                            std::cerr << "Column not found.\n";
                            return;
                          };
                          const std::span<int> values(int_v.data() + i * nrow, nrow);
                          std::sort(idx.begin(), idx.end(),
                          [&](size_t a, size_t b) {
                              return values[a] < values[b];
                          });
    
                          break;
    
                      };
              case 'u': {
    
                          while (n != matr_idx[4][i]) {
                            i += 1;
                          }
                          if (i == matr_idx[4].size()) {
                            std::cerr << "Column not found.\n";
                            return;
                          };
                          const std::span<unsigned int> values(uint_v.data() + i * nrow, nrow);
                          std::sort(idx.begin(), idx.end(),
                          [&](size_t a, size_t b) {
                              return values[a] < values[b];
                          });
    
                          break;
    
                        };
              case 'd': {
    
                          while (n != matr_idx[5][i]) {
                            i += 1;
                          }
                          if (i == matr_idx[5].size()) {
                            std::cerr << "Column not found.\n";
                            return;
                          };
                          const std::span<double> values(dbl_v.data() + i * nrow, nrow);
                          std::sort(idx.begin(), idx.end(),
                          [&](size_t a, size_t b) {
                              return values[a] < values[b];
                          });
    
                          break;
    
                        };
              default: {
                         std::cerr << "Type unknowk in sort_by\n";
                         return;
                       }
            };
          } else if constexpr (!asc) {
            switch(type_refv[n]) {
              case 's': {
    
                          while (n != matr_idx[0][i]) {
                            i += 1;
                          }
    
                          if (i == matr_idx[0].size()) {
                            std::cerr << "Column not found.\n";
                            return;
                          };
    
                          const std::span<std::string> values(str_v.data() + i * nrow, nrow);
                          std::sort(idx.begin(), idx.end(),
                          [&](size_t a, size_t b) {
                              return values[a] > values[b];
                          });
    
                          break;
    
                        };
              case 'c': {
    
                          while (n != matr_idx[1][i]) {
                            i += 1;
                          }
    
                          if (i == matr_idx[1].size()) {
                            std::cerr << "Column not found.\n";
                            return;
                          };
    
                          const std::span<char> values(chr_v.data() + i * nrow, nrow);
                          std::sort(idx.begin(), idx.end(),
                          [&](size_t a, size_t b) {
                              return values[a] > values[b];
                          });
    
                          break;
    
                        };
              case 'b': {
    
                          while (n != matr_idx[2][i]) {
                            i += 1;
                          }
    
                          if (i == matr_idx[2].size()) {
                            std::cerr << "Column not found.\n";
                            return;
                          };
    
                          std::vector<bool> values(bool_v.begin() + i * nrow, 
                                          bool_v.begin() + (i + 1) * nrow);
                          std::sort(idx.begin(), idx.end(),
                          [&](size_t a, size_t b) {
                              return values[a] > values[b];
                          });
    
                          break;
    
                        };
              case 'i': {
    
                          while (n != matr_idx[3][i]) {
                            i += 1;
                          }
    
                          if (i == matr_idx[3].size()) {
                            std::cerr << "Column not found.\n";
                            return;
                          };
    
                          const std::span<int> values(int_v.data() + i * nrow, nrow);
                          std::sort(idx.begin(), idx.end(),
                          [&](size_t a, size_t b) {
                              return values[a] > values[b];
                          });
    
                          break;
    
                      };
              case 'u': {
    
                          while (n != matr_idx[4][i]) {
                            i += 1;
                          }
    
                          if (i == matr_idx[4].size()) {
                            std::cerr << "Column not found.\n";
                            return;
                          };
    
                          const std::span<unsigned int> values(uint_v.data() + i * nrow, nrow);
                          std::sort(idx.begin(), idx.end(),
                          [&](size_t a, size_t b) {
                              return values[a] > values[b];
                          });
    
                          break;
    
                        };
              case 'd': {
    
                          while (n != matr_idx[5][i]) {
                            i += 1;
                          }
    
                          if (i == matr_idx[5].size()) {
                            std::cerr << "Column not found.\n";
                            return;
                          };
    
                          const std::span<double> values(dbl_v.data() + i * nrow, nrow);
                          std::sort(idx.begin(), idx.end(),
                          [&](size_t a, size_t b) {
                              return values[a] > values[b];
                          });
    
                          break;
    
                        };
              default: {
                         std::cerr << "Type unknowk in sort_by\n";
                         return;
                       }
            };
          };
          unsigned int pos_vl;
          unsigned int pos_vl2;
          std::vector<std::string> tmp_str_vec = str_v;
          std::vector<std::string> str_v2(nrow);
          i2 = 0;
          for (auto& el : matr_idx[0]) {
            pos_vl = i2 * nrow;
            for (i = 0; i < nrow; i += 1) {
              pos_vl2 = idx[i];
              str_v2[i] = tmp_val_refv[el][pos_vl2];
              tmp_str_vec[pos_vl + i] = str_v[pos_vl + pos_vl2];
            };
            tmp_val_refv[el] = str_v2;
            i2 += 1;
          };
          str_v = tmp_str_vec;
          std::vector<char> tmp_chr_vec = chr_v;
          i2 = 0;
          for (auto& el : matr_idx[1]) {
            pos_vl = i2 * nrow;
            for (i = 0; i < nrow; i += 1) {
              pos_vl2 = idx[i];
              str_v2[i] = tmp_val_refv[el][pos_vl2];
              tmp_chr_vec[pos_vl + i] = chr_v[pos_vl + pos_vl2];
            };
            tmp_val_refv[el] = str_v2;
            i2 += 1;
          };
          chr_v = tmp_chr_vec;
          std::vector<bool> tmp_bool_vec = bool_v;
          i2 = 0;
          for (auto& el : matr_idx[2]) {
            pos_vl = i2 * nrow;
            for (i = 0; i < nrow; i += 1) {
              pos_vl2 = idx[i];
              str_v2[i] = tmp_val_refv[el][pos_vl2];
              tmp_bool_vec[pos_vl + i] = bool_v[pos_vl + pos_vl2];
            };
            tmp_val_refv[el] = str_v2;
            i2 += 1;
          };
          bool_v = tmp_bool_vec;
          std::vector<int> tmp_int_vec = int_v;
          i2 = 0;
          for (auto& el : matr_idx[3]) {
            pos_vl = i2 * nrow;
            for (i = 0; i < nrow; i += 1) {
              pos_vl2 = idx[i];
              str_v2[i] = tmp_val_refv[el][pos_vl2];
              tmp_int_vec[pos_vl + i] = int_v[pos_vl + pos_vl2];
            };
            tmp_val_refv[el] = str_v2;
            i2 += 1;
          };
          int_v = tmp_int_vec;
          std::vector<unsigned int> tmp_uint_vec = uint_v;
          i2 = 0;
          for (auto& el : matr_idx[4]) {
            pos_vl = i2 * nrow;
            for (i = 0; i < nrow; i += 1) {
              pos_vl2 = idx[i];
              str_v2[i] = tmp_val_refv[el][pos_vl2];
              tmp_uint_vec[pos_vl + i] = uint_v[pos_vl + pos_vl2];
            };
            tmp_val_refv[el] = str_v2;
            i2 += 1;
          };
          uint_v = tmp_uint_vec;
          std::vector<double> tmp_dbl_vec = dbl_v;
          i2 = 0;
          for (auto& el : matr_idx[5]) {
            pos_vl = i2 * nrow;
            for (i = 0; i < nrow; i += 1) {
              pos_vl2 = idx[i];
              str_v2[i] = tmp_val_refv[el][pos_vl2];
              tmp_dbl_vec[pos_vl + i] = dbl_v[pos_vl + pos_vl2];
            };
            tmp_val_refv[el] = str_v2;
            i2 += 1;
          };
          dbl_v = tmp_dbl_vec;
        };
    
        void concat(Dataframe& obj) {
          const unsigned int& ncol2 = obj.get_ncol();
          if (ncol != ncol2) {
            std::cerr << "Can't concatenate 2 dataframes with different number of columns\n";
            return;
          };
          const std::vector<char> type_refv2 = obj.get_typecol();
          if (type_refv != type_refv2) {
            std::cerr << "Can't concatenate 2 dataframes with different column type\n";
            return;
          };
    
          const std::vector<std::string>& str_v2 = obj.get_str_vec();
          const std::vector<char>& chr_v2 = obj.get_chr_vec();
          const std::vector<bool>& bool_v2 = obj.get_bool_vec();
          const std::vector<int>& int_v2 = obj.get_int_vec();
          const std::vector<unsigned int>& uint_v2 = obj.get_uint_vec();
          const std::vector<double>& dbl_v2 = obj.get_dbl_vec();
    
          const std::vector<std::vector<std::string>>& tmp_val_refv2 = obj.get_tmp_val_refv();
     
          size_t i2 = 0;
          size_t i = 0;
    
          const unsigned int& nrow2 = obj.get_nrow();
          unsigned int pre_nrow = nrow;
          nrow += nrow2;
    
          str_v.reserve(str_v.size() + str_v2.size());
          for (auto& el : matr_idx[0]) {
            tmp_val_refv[el].reserve(nrow);
            tmp_val_refv[el].insert(tmp_val_refv[el].end(), 
                            tmp_val_refv2[el].begin(),
                            tmp_val_refv2[el].end());
            str_v.insert(str_v.begin() + (i2 + 1) * pre_nrow, 
                            str_v2.begin() + i * nrow2,
                            str_v2.begin() + (i + 1) * nrow2);
            i2 += 2;
            i += 1;
          };
    
          i2 = 0;
          i = 0;
          chr_v.reserve(chr_v.size() + chr_v2.size());
    
          for (auto& el : matr_idx[1]) {
            tmp_val_refv[el].reserve(nrow);
            tmp_val_refv[el].insert(tmp_val_refv[el].end(), 
                            tmp_val_refv2[el].begin(),
                            tmp_val_refv2[el].end());
            chr_v.insert(chr_v.begin() + (i2 + 1) * pre_nrow, 
                            chr_v2.begin() + i * nrow2,
                            chr_v2.begin() + (i + 1) * nrow2);
            i2 += 2;
            i += 1;
          };
    
          i2 = 0;
          i = 0;
          bool_v.reserve(bool_v.size() + bool_v2.size());
    
          for (auto& el : matr_idx[2]) {
            tmp_val_refv[el].reserve(nrow);
            tmp_val_refv[el].insert(tmp_val_refv[el].end(), 
                            tmp_val_refv2[el].begin(),
                            tmp_val_refv2[el].end());
            bool_v.insert(bool_v.begin() + (i2 + 1) * pre_nrow, 
                            bool_v2.begin() + i * nrow2,
                            bool_v2.begin() + (i + 1) * nrow2);
            i2 += 2;
            i += 1;
          };
    
          i2 = 0;
          i = 0;
          int_v.reserve(int_v.size() + int_v2.size());
    
          for (auto& el : matr_idx[3]) {
            tmp_val_refv[el].reserve(nrow);
            tmp_val_refv[el].insert(tmp_val_refv[el].end(), 
                            tmp_val_refv2[el].begin(),
                            tmp_val_refv2[el].end());
            int_v.insert(int_v.begin() + (i2 + 1) * pre_nrow, 
                            int_v2.begin() + i * nrow2,
                            int_v2.begin() + (i + 1) * nrow2);
            i2 += 2;
            i += 1;
          };
    
          i2 = 0;
          i = 0;
          uint_v.reserve(uint_v.size() + uint_v2.size());
          
          for (auto& el : matr_idx[4]) {
            tmp_val_refv[el].reserve(nrow);
            tmp_val_refv[el].insert(tmp_val_refv[el].end(), 
                            tmp_val_refv2[el].begin(),
                            tmp_val_refv2[el].end());
            uint_v.insert(uint_v.begin() + (i2 + 1) * pre_nrow, 
                            uint_v2.begin() + i * nrow2,
                            uint_v2.begin() + (i + 1) * nrow2);
            i2 += 2;
            i += 1;
          };
    
          i2 = 0;
          i = 0;
          dbl_v.reserve(dbl_v.size() + dbl_v2.size());
          
          for (auto& el : matr_idx[5]) {
            tmp_val_refv[el].reserve(nrow);
            tmp_val_refv[el].insert(tmp_val_refv[el].end(), 
                            tmp_val_refv2[el].begin(),
                            tmp_val_refv2[el].end());
            dbl_v.insert(dbl_v.begin() + (i2 + 1) * pre_nrow, 
                            dbl_v2.begin() + i * nrow2,
                            dbl_v2.begin() + (i + 1) * nrow2);
            i2 += 2;
            i += 1;
          };
    
        };
    
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

#if 0

//@T Dataframe.readf
//@U template <unsigned int strt_row = 0, 
//@U          unsigned int end_row = 0, 
//@U          unsigned int CORES = 1, 
//@U          bool WARMING = 0, 
//@U          bool CLEAN = 0, 
//@U          bool MEM_CLEAN = 0>
//@U     void readf(std::string &file_name, char delim = ',', 
//@U                     bool header_name = 1, char str_context = '\'')
//@X
//@D Import a csv as a Dataframe object.
//@A file_name : is the file_name of the csv to read
//@A delim : is the column delimiter
//@A header_name : is if the first row is in fact the column names
//@A str_context_begin : is the first symbol of a quote, (to not take in count a comma as a new column if it is in a quote for example)
//@A str_context_end : is the end symbol for a quote context
//@A strt_row : is the first row to read, defaults to 0
//@A end_row : is the last row to read, defaults to max (value of 0)
//@A CORES : are the number of cores used for parsing and type inference
//@A WARMING : enables cache warming, might help if a lot of delimiters by column
//@A MEM_CLEAN : free unnused memory, might be slower
//@X
//@E Dataframe obj1;
//@E std::string file_name = "teste_dataframe.csv";
//@E obj1.readf<3, 8>(file_name); reads from the 3thrd to the 8nth row
//@X

//@T Dataframe.readf_trim
//@U template<unsigned int strt_row = 0, unsigned int end_row = 0>
//@U void readf_trim(std::string &file_name, char delim = ',', bool header_name = 1, char str_context_begin = '\'', char str_context_end = '\'')
//@X
//@D Import a csv as a Dataframe object. Automatically trim the value (removes extra spaces before and after)
//@A file_name : is the file_name of the csv to read
//@A delim : is the column delimiter
//@A header_name : is if the first row is in fact the column names
//@A str_context_begin : is the first symbol of a quote, (to not take in count a comma as a new column if it is in a quote for example)
//@A str_context_end : is the end symbol for a quote context
//@A strt_row : is the first row to read, defaults to 0
//@A end_row : is the last row to read, defaults to max (value of 0)
//@X
//@E Dataframe obj1;
//@E std::string file_name = "teste_dataframe2.csv";
//@E obj1.readf_trim<0, 8>(file_name); // reads until the 8nth row
//@X

//@T Dataframe.readf_lambda
//@U template<unsigned int strt_row = 0, unsigned int end_row = 0>
//@U void readf_lambda(std::string &file_name, void (&f)(std::string&), char delim = ',', bool header_name = 1, char str_context_begin = '\'', char str_context_end = '\'')
//@X
//@D Import a csv as a Dataframe object. Applies a custom function to all values from the csv before parsing it as appropriate data types.
//@A file_name : is the file_name of the csv to read
//@A f : is your custom function (void, must take a std:string as argument)
//@A delim : is the column delimiter
//@A header_name : is if the first row is in fact the column names
//@A str_context_begin : is the first symbol of a quote, (to not take in count a comma as a new column if it is in a quote for example)
//@A str_context_end : is the end symbol for a quote context
//@A strt_row : is the first row to read, defaults to 0
//@A end_row : is the last row to read, defaults to max (value of 0)
//@X
//@E void myfunc (std::string &x) {
//@E   x.push_back('L');
//@E };
//@E Dataframe obj1;
//@E std::string file_name = "teste_dataframe2.csv";
//@E obj1.readf_lambda<3, 0>(file_name, myfunc); // reads from the thirs row
//@X

//@T Dataframe.readf_alrd
//@U template<unsigned int strt_row = 0, unsigned int end_row = 0>
//@U void readf_alrd(std::string &file_name, std::vector&lt;string&gt;& dtype, char delim = ',', bool header_name = 1, char str_context_begin = '\'', char str_context_end = '\'')
//@X
//@D Import a csv as a Dataframe object. This function only makes sense if you know in advance the column data types (faster than semantical analysis). The column data types are contiguously described in <code>dtype</code> argument. The values are 's' (string), 'c' (char), 'b' (bool), 'i' (int), 'u' (unsigned int) and 'd' (double)
//@A file_name : is the file_name of the csv to read
//@A dtype : is the string vector containing all column data types
//@A f : is your custom function (void, must take a std:string as argument)
//@A delim : is the column delimiter
//@A header_name : is if the first row is in fact the column names
//@A str_context_begin : is the first symbol of a quote, (to not take in count a comma as a new column if it is in a quote for example)
//@A str_context_end : is the end symbol for a quote context
//@A strt_row : is the first row to read, defaults to 0
//@A end_row : is the last row to read, defaults to max (value of 0)
//@X
//@E std::string dvec = "dsusic";
//@E Dataframe obj1;
//@E std::string file_name = "teste_dataframe2.csv";
//@E obj1.readf_lambda(file_name, dvec); //reads the entire file
//@X

//@T Dataframe.writef
//@U void writef(std::string &file_name, char delim = ',', bool header_name = 1, char str_context_bgn = '\'', char str_context_end = '\'')
//@X
//@D Write a dataframe object into a csv file.
//@A file_name : is the file name to write data into
//@A delim : is the column delimiter
//@A header_name : 1 to write the column names, 0 else
//@A str_context_begin : is the first symbol of a quote, (to not take in count a comma as a new column if it is in a quote for example)
//@A str_context_end : is the end symbol for a quote context
//@X
//@E // after reading teste_dataframe.csv as obj1
//@E std::string out_file = "out.csv";
//@E obj1.writef(out_file);
//@X

//@T Dataframe.display
//@U void display();
//@X
//@D Print the current dataframe.
//@A no : no
//@X
//@E // after reading teste_dataframe.csv as obj1
//@E obj1.display();
//@E &lt;uint&gt; &lt;uint&gt; &lt;uint&gt; &lt;str&gt; &lt;int&gt; &lt;char&gt;
//@E col1 col2 col3 col4 col5 col6
//@E :0:  1    2    3    aa   5    z
//@E :1:  6    7    8    bb   10   e
//@E :2:  1    2    3    cc   5    h
//@E :3:  6    7    8    uu   10   a
//@E :4:  1    2    3    s4   -5   q
//@E :5:  6    7    8    s9   10   p
//@E :6:  1    2    3    a4   5    j
//@E :7:  6    7    8    m9   10   i
//@E :8:  6    7    8    s9   10   p
//@E :9:  1    2    3    a4   5    j
//@E :10: 6    7    8    m9   10   i
//@E :11: 6    7    8    m9   10   i
//@E :12: 6    7    8    s9   10   p
//@E :13: 1    2    3    a4   5    j
//@E :14: 6    7    8    m9   10   i
//@M img_dataframe.jpg
//@X

//@T Dataframe.display_filter
//@U void display_filter(std::vector&lt;bool&gt; &x, std::vector&lt;int&gt; &colv)
//@X
//@D Print the current dataframe. Works seemlessly with <code>Dataframe.view_colnb()</code> to efficiently create boolean vector to filter rows, see example.
//@A x : is the boolean vector filtering the rows to display
//@A colv : is the int vector representing the column index of the column to keep, <code>{-1}</code> to keep all columns
//@X
//@E // after reading teste_dataframe.csv as obj1
//@E
//@E unsigned int n = 1;
//@E
//@E std::vector&lt;int&gt; cols = {-1};
//@E
//@E auto col = obj1.view_colnb(n);
//@E
//@E std::vector&lt;bool&gt; mask; 
//@E
//@E std::visit([&mask](auto&& span) {
//@E     mask.resize(span.size());
//@E     for (size_t i = 0; i &lt; span.size(); ++i)
//@E         mask[i] = span[i] &gt; 3;
//@E }, col);
//@E 
//@E obj1.display_filter(mask, cols);
//@E
//@E     &lt;uint&gt; &lt;uint&gt; &lt;uint&gt; &lt;str&gt; &lt;int&gt; &lt;char&gt;
//@E     col1   col2   col3   col4  col5  col6
//@E :1:  6      7      8      bb    10    e
//@E :3:  6      7      8      uu    10    a
//@E :5:  6      7      8      s9    10    p
//@E :7:  6      7      8      m9    10    i
//@E :8:  6      7      8      s9    10    p
//@E :10: 6      7      8      m9    10    i
//@E :11: 6      7      8      m9    10    i
//@E :12: 6      7      8      s9    10    p
//@E :14: 6      7      8      m9    10    i
//@X

//@T Dataframe.display_filter_idx
//@U void display_filter_idx(std::vector&lt;int&gt; &x, std::vector&lt;int&gt; &colv)
//@X
//@D Print the current dataframe. Works seemlessly with <code>Dataframe.view_colnb()</code> to efficiently create an int vector to filter rows, see example.
//@A x : is the int vector filtering the rows to display, the vector length is not forced to match the dataframe row number. <code>{-1}</code> to keep all rows in default order
//@A colv : is the int vector representing the column index of the column to keep, <code>{-1}</code> to keep all columns
//@X
//@E
//@E  Dataframe obj1;
//@E  std::string fname = "csv_test/outb.csv";
//@E  obj1.readf&lt;3, 0&gt;(fname);
//@E  obj1.display();
//@E     &lt;str&gt; &lt;uint&gt; &lt;uint&gt; &lt;uint&gt; &lt;str&gt;
//@E     col1  col2   col3   col4   col5
//@E :0:  id4   6      7      8      uu
//@E :1:  id5   1      2      3      s4
//@E :2:  id6   6      7      8      s9
//@E :3:  id7   1      2      3      a4
//@E :4:  id8   6      7      8      m9
//@E :5:  id9   6      7      8      s9
//@E :6:  id10  1      2      3      a4
//@E :7:  id11  6      7      8      m9
//@E :8:  id12  6      7      8      m9
//@E :9:  id13  6      7      8      s9
//@E :10: id14  1      2      3      NA
//@E :11: id15  16     7      8      m9
//@E  std::vector&lt;int&gt; rvec = {3, 2, 5, 3, 6, 7, 6, 5, 8, 9, 11, 10};
//@E  std::vector&lt;int&gt; cvec = {-1};
//@E  obj1.display_filter_idx(rvec, cvec);
//@E
//@E     &lt;str&gt; &lt;uint&gt; &lt;uint&gt; &lt;uint&gt; &lt;str&gt;
//@E     col1  col2   col3   col4   col5
//@E :0:  id7   1      2      3      a4
//@E :1:  id6   6      7      8      s9
//@E :2:  id9   6      7      8      s9
//@E :3:  id7   1      2      3      a4
//@E :4:  id10  1      2      3      a4
//@E :5:  id11  6      7      8      m9
//@E :6:  id10  1      2      3      a4
//@E :7:  id9   6      7      8      s9
//@E :8:  id12  6      7      8      m9
//@E :9:  id13  6      7      8      s9
//@E :10: id15  16     7      8      m9
//@E :11: id14  1      2      3      NA
//@X

//@T Dataframe.get_dataframe
//@U void get_dataframe(std::vector&lt;int&gt; &cols, Dataframe &cur_obj)
//@X
//@D Allow to copy a dataframe choosing columns (by index) of the copied dataframe. 
//@A cols : is the vector of the index of the columns to copy (<code>{-1}</code>) for all
//@A cur_obj : is the dataframe that will contain all the rows and columns of the copied dataframe
//@X
//@E // after reading teste_dataframe.csv as obj1
//@E Dataframe obj2;
//@E std::vector&lt;int&gt; idx_cols2 = {1, 2, 3};
//@E obj2.get_dataframe(idx_cols2, obj1);
//@E 
//@E obj2.display();
//@E     col1 col2 col3
//@E :0:  2    3    aa
//@E :1:  7    8    bb
//@E :2:  2    3    cc
//@E :3:  7    8    uu
//@E :4:  2    3    s4
//@E :5:  7    8    s9
//@E :6:  2    3    a4
//@E :7:  7    8    m9
//@E :8:  7    8    s9
//@E :9:  2    3    a4
//@E :10: 7    8    m9
//@E :11: 7    8    m9
//@E :12: 7    8    s9
//@E :13: 2    3    a4
//@E :14: 7    8    m9
//@X

//@T Dataframe.get_dataframe_filter
//@U void get_dataframe_filter(std::vector&lt;int&gt; &cols, Dataframe &cur_obj, std::vector&lt;bool&gt; &mask)
//@X
//@D Allow to copy a dataframe choosing columns (by index) of the copied dataframe, while applying a mask on the desired rows. 
//@A cols : is the vector of the index of the columns to copy (<code>{-1}</code>) for all
//@A cur_obj : is the dataframe that will contain all the rows and columns of the copied dataframe
//@A mask : is the boolean mask vector
//@X
//@E // after reading teste_dataframe.csv as obj1
//@E unsigned int n = 3;
//@E auto vec = obj1.view_colnb(n);
//@E std::vector&lt;bool&gt; vmask = {};
//@E std::visit([&vmask](auto&& span) {
//@E     vmask.resize(span.size());
//@E     for (size_t i = 0; i &lt; span.size(); ++i)
//@E         vmask[i] = span[i] &gt; 4;
//@E }, vec);
//@E Dataframe obj2;
//@E std::vector&lt;int&gt; idx_cols2 = {1, 2, 3};
//@E obj2.get_dataframe_filter(idx_cols2, obj1, vmask);
//@E 
//@E obj2.display();
//@E     col1 col2 col3
//@E :1:  7    8    bb
//@E :3:  7    8    uu
//@E :5:  7    8    s9
//@E :7:  7    8    m9
//@E :8:  7    8    s9
//@E :10: 7    8    m9
//@E :11: 7    8    m9
//@E :12: 7    8    s9
//@E :14: 7    8    m9
//@X

//@T Dataframe.get_dataframe_filter_idx
//@U void get_dataframe_filter_idx(std::vector&lt;int&gt; &cols, Dataframe &cur_obj, std::vector&lt;int&gt; &mask)
//@X
//@D Allow to copy a dataframe choosing columns (by index) of the copied dataframe, while applying an index mask on the desired rows. 
//@A cols : is the vector of the index of the columns to copy (<code>{-1}</code>) for all
//@A cur_obj : is the dataframe that will contain all the rows and columns of the copied dataframe
//@A mask : is the index mask vector, <code>{-1}</code> to keep all rows
//@X
//@E 
//@E   Dataframe obj1, obj2, obj3;
//@E   std::string fname = "csv_test/outb.csv";
//@E   obj1.readf&lt;3, 0&gt;(fname);
//@E   obj1.display();
//@E
//@E     &lt;str&gt; &lt;uint&gt; &lt;uint&gt; &lt;uint&gt; &lt;str&gt;
//@E     col1  col2   col3   col4   col5
//@E :0:  id4   6      7      8      uu
//@E :1:  id5   1      2      3      s4
//@E :2:  id6   6      7      8      s9
//@E :3:  id7   1      2      3      a4
//@E :4:  id8   6      7      8      m9
//@E :5:  id9   6      7      8      s9
//@E :6:  id10  1      2      3      a4
//@E :7:  id11  6      7      8      m9
//@E :8:  id12  6      7      8      m9
//@E :9:  id13  6      7      8      s9
//@E :10: id14  1      2      3      NA
//@E :11: id15  16     7      8      m9
//@E
//@E   std::vector&lt;int&gt; rvec = {3, 2, 5, 3, 6, 7, 6, 5, 8, 9, 11, 10};
//@E   std::vector&lt;int&gt; cvec = {-1};
//@E   obj2.get_dataframe_filter_idx(cvec, obj1, rvec);
//@E   obj2.display();
//@E
//@E     &lt;str&gt; &lt;uint&gt; &lt;uint&gt; &lt;uint&gt; &lt;str&gt;
//@E     col1  col2   col3   col4   col5
//@E :0:  id7   1      2      3      a4
//@E :1:  id6   6      7      8      s9
//@E :2:  id9   6      7      8      s9
//@E :3:  id7   1      2      3      a4
//@E :4:  id10  1      2      3      a4
//@E :5:  id11  6      7      8      m9
//@E :6:  id10  1      2      3      a4
//@E :7:  id9   6      7      8      s9
//@E :8:  id12  6      7      8      m9
//@E :9:  id13  6      7      8      s9
//@E :10: id15  16     7      8      m9
//@E :11: id14  1      2      3      NA
//@E
//@X

//@T Dataframe.view_colnb
//@U using ColumnView = std::variant&lt;
//@U         std::span&lt;const int&gt;,
//@U         std::span&lt;const unsigned int&gt;,
//@U         std::span&lt;const double&gt;
//@U     &gt;;
//@U 
//@U     ColumnView view_colnb(unsigned int &x) const
//@X
//@D Allow to get the reference of a int, unsigned int or double column as a span&lt;T&gt;, by column index (>= C++ 20).
//@A rows : is a vector containing the row indices to copy (<code>{-1}</code>) for all. Intended to be used for creating boolean vecotr by your own functions, to maybe filter data later with <code>Dataframe.display_filter(), Dataframe.get_dataframe_filter(), Dataframe.get_col_filter, ...</code>
//@A x : is the index of the column to get the ref from
//@X
//@E // after reading teste_dataframe.csv as obj1
//@E unsigned int n = 1;
//@E 
//@E auto col = obj1.view_colnb(n);
//@X

//@T Dataframe.view_colstr
//@U std::span&lt;const std::string&gt; view_colstr(unsigned int &x) const 
//@X
//@D Allow to get the reference of a std::string column by column index.
//@A x : is the index of the column to get the ref from
//@X
//@E // after reading teste_dataframe.csv as obj1
//@E unsigned int n = 3;
//@E 
//@E auto col = obj1.view_colstr(n);
//@X

//@T Dataframe.view_colchr
//@U std::span&lt;const std::string&gt; view_colchr(unsigned int &x) const 
//@X
//@D Allow to get the reference of a char column by column index.
//@A x : is the index of the column to get the ref from
//@X
//@E // after reading teste_dataframe.csv as obj1
//@E unsigned int n = 5;
//@E 
//@E auto col = obj1.view_colchr(n);
//@X

//@T Dataframe.view_colint
//@U std::span&lt;const std::string&gt; view_colint(unsigned int &x) const 
//@X
//@D Allow to get the reference of an int column by column index.
//@A x : is the index of the column to get the ref from
//@X
//@E // after reading teste_dataframe.csv as obj1
//@E unsigned int n = 4;
//@E 
//@E auto col = obj1.view_colstr(n);
//@X

//@T Dataframe.view_coluint
//@U std::span&lt;const std::string&gt; view_coluint(unsigned int &x) const 
//@X
//@D Allow to get the reference of an uint column by column index.
//@A x : is the index of the column to get the ref from
//@X
//@E // after reading teste_dataframe.csv as obj1
//@E unsigned int n = 0;
//@E 
//@E auto col = obj1.view_colchr(n);
//@X

//@T Dataframe.view_coldbl
//@U std::span&lt;const std::string&gt; view_coldbl(unsigned int &x) const 
//@X
//@D Allow to get the reference of a double column by column index.
//@A x : is the index of the column to get the ref from
//@X
//@E // after reading teste_dataframe.csv as obj1
//@E unsigned int n = 1;
//@E 
//@E auto col = obj1.view_coldbl(n);
//@X

//@T Dataframe.get_col
//@U template &lt;typename T&gt;
//@U void get_col(unsigned int x, std::vector&lt;T&gt; &rtn_v)
//@X
//@D Allow to copy a column as a vector, by column index.
//@A x : is the index of the column to copy
//@A rtn_v : is the vector that will contain the column to copy
//@X
//@E // after reading teste_dataframe.csv as obj1
//@E std::vector&lt;std::string&gt; outv2 = {};
//@E obj1.get_col(3, outv2);
//@X

//@T Dataframe.get_col_filter
//@U template &lt;typename T&gt;
//@U void get_col_filter(unsigned int x, std::vector&lt;T&gt; &rtn_v, std::vector&lt;bool&gt; &mask)
//@X
//@D Allow to copy a column as a vector, by column index with a boolean vector mask to keep the desired lines.
//@A x : is the index of the column to copy
//@A rtn_v : is the vector that will contain the column to copy
//@X
//@E // after reading teste_dataframe.csv as obj1
//@E std::vector&lt;std::string&gt; outv2 = {};
//@E unsigned int n = 0;
//@E std::vector&lt;bool&gt; mask;
//@E unsigned int nrow = obj1.get_nrow();
//@E mask.reserve(nrow);
//@E auto vec = obj1.view_coluint(n);
//@E std::visit([&mask](auto&& span) {
//@E     mask.resize(span.size());
//@E     for (size_t i = 0; i &lt; span.size(); ++i)
//@E         mask[i] = span[i] &gt; 3;
//@E }, vec);
//@E obj1.get_col(3, outv2, mask);
//@X

//@T Dataframe.fapply
//@U template &lt;typename T&gt;
//@U void fapply(void (&f)(T&), unsigned int &n)
//@X
//@D Apply whatever function to all elements of a column. See example
//@A f : is the function to apply
//@A n : is the column index
//@X
//@E 
//@E   obj1.display();
//@E
//@E     &lt;str&gt; &lt;uint&gt;
//@E     col1  col2
//@E :0:  id1   1
//@E :1:  id2   6
//@E :2:  id4   6
//@E :3:  id5   1
//@E :4:  id6   6
//@E :5:  id7   1
//@E :6:  id8   6
//@E :7:  id9   6
//@E :8:  id11  6
//@E :9:  id12  6
//@E :10: id14  1
//@E :11: id15  16
//@E 
//@E   unsigned int n = 1;
//@E   obj1.fapply(mfunc, n);
//@E 
//@E   obj1.display();
//@E
//@E     &lt;str&gt; &lt;uint&gt;
//@E     col1  col2
//@E :0:  id1   2
//@E :1:  id2   7
//@E :2:  id4   7
//@E :3:  id5   2
//@E :4:  id6   7
//@E :5:  id7   2
//@E :6:  id8   7
//@E :7:  id9   7
//@E :8:  id11  7
//@E :9:  id12  7
//@E :10: id14  2
//@E :11: id15  17
//@E
//@X

//@T Dataframe.get_nrow
//@U unsigned int get_nrow();
//@X
//@D Returns the number of rows for the associated dataframe.
//@E // after reading teste_dataframe.csv as obj1
//@E unsigned int nrow = obj1.get_nrow();
//@E 15
//@X

//@T Dataframe.get_ncol
//@U unsigned int get_ncol();
//@X
//@D Returns the number of columns for the associated dataframe.
//@E // after reading teste_dataframe.csv as obj1
//@E unsigned int ncol = obj1.get_ncol();
//@E 6
//@X

//@T Dataframe.get_rowname
//@U std::vector&lt;std::string&gt; get_rowname();
//@X
//@D Returns the rowname of the associated dataframe.
//@E // after reading teste_dataframe.csv as obj1
//@E std::vector&lt;std::string&gt; row_names = obj1.get_rowname();
//@E nothing becuase obj1 has no rownames
//@X

//@T Dataframe.get_colname
//@U std::vector&lt;std::string&gt; get_colname();
//@X
//@D Returns the colname of the associated dataframe.
//@E // after reading teste_dataframe.csv as obj1
//@E std::vector&lt;std::string&gt; col_names = obj1.get_colname();
//@E col1 col2 col3 col4 col5 col6
//@X

//@T Dataframe.set_rowname
//@U void set_rowname(std::vector&lt;std::string&gt; &x);
//@X
//@D Set rowname to the associated dataframe.
//@E // after reading teste_dataframe.csv as obj1
//@E std::vector&lt;std::string&gt; row_names = {"n1", "n2", "n3"..."n15"};
//@E obj1.set_rowname(row_names);
//@X

//@T Dataframe.set_colname
//@U void set_colname(std::vector&lt;std::string&gt; &x);
//@X
//@D Set colname to the associated dataframe.
//@E // after reading teste_dataframe.csv as obj1
//@E std::vector&lt;std::string&gt; col_names = {"col1", "col2", "col3", "col4", 
//@E "col5", "col6"};
//@E obj1.set_colname();
//@X

//@T Dataframe.replace_col
//@U template &lt;typename T&gt; void replace_col(std::vector&lt;T&gt; &x, unsigned int &colnb)
//@X
//@D Replace a column of the associated dataframe.
//@A x : is the column (as vector) that will replace the dataframe column
//@A colnb : is the index of the column to replace
//@X
//@E // after reading teste_dataframe.csv as obj1
//@E 
//@E std::vector&lt;unsigned int&gt; rpl_col = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 
//@E                                       10, 11, 12, 13, 14};
//@E unsigned int col = 0;
//@E obj1.replace_col(rpl_col, col);
//@E obj1.display();
//@E      &lt;uint&gt; &lt;uint&gt; &lt;uint&gt; &lt;str&gt; &lt;int&gt; &lt;char&gt;
//@E      col1   col2   col3   col4  col5  col6
//@E  :0:  0      2      3      aa    5     z
//@E  :1:  1      7      8      bb    10    e
//@E  :2:  2      2      3      cc    5     h
//@E  :3:  3      7      8      uu    10    a
//@E  :4:  4      2      3      s4    -5    q
//@E  :5:  5      7      8      s9    10    p
//@E  :6:  6      2      3      a4    5     j
//@E  :7:  7      7      8      m9    10    i
//@E  :8:  8      7      8      s9    10    p
//@E  :9:  9      2      3      a4    5     j
//@E  :10: 10     7      8      m9    10    i
//@E  :11: 11     7      8      m9    10    i
//@E  :12: 12     7      8      s9    10    p
//@E  :13: 13     2      3      NA    5     j
//@E  :14: 14     7      8      m9    10    i
//@X

//@T Dataframe.add_col
//@U template &lt;typename T&gt; void add_col(std::vector&lt;T&gt; &x, std::string name = "NA")
//@X
//@D Add a column int, unsigned int, bool, double, char or string type to the associated dataframe
//@A x : is the column to add
//@A name : is the column to add name
//@X
//@E // after reading teste_dataframe.csv as obj1
//@E std::vector&lt;unsigned int&gt; rpl_col = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 
//@E                                       10, 11, 12, 13, 14};
//@E obj1.add_col(rpl_col);
//@E obj1.display();
//@E     &lt;uint&gt; &lt;uint&gt; &lt;uint&gt; &lt;str&gt; &lt;int&gt; &lt;char&gt; &lt;uint&gt;
//@E     col1   col2   col3   col4  col5  col6   NA
//@E :0:  1      2      3      aa    5     z      0
//@E :1:  6      7      8      bb    10    e      1
//@E :2:  1      2      3      cc    5     h      2
//@E :3:  6      7      8      uu    10    a      3
//@E :4:  1      2      3      s4    -5    q      4
//@E :5:  6      7      8      s9    10    p      5
//@E :6:  1      2      3      a4    5     j      6
//@E :7:  6      7      8      m9    10    i      7
//@E :8:  6      7      8      s9    10    p      8
//@E :9:  1      2      3      a4    5     j      9
//@E :10: 6      7      8      m9    10    i      10
//@E :11: 6      7      8      m9    10    i      11
//@E :12: 6      7      8      s9    10    p      12
//@E :13: 1      2      3      NA    5     j      13
//@E :14: 6      7      8      m9    10    i      14
//@X

//@T Dataframe.rm_col
//@U void rm_col(std::vector&lt;unsigned int&gt; &nbcolv)
//@X
//@D Removes columns from associated dataframe.
//@A nbcolv : is a vector containing all the indices of the columns to erase from the associated dataframe. The indices must be sorted descendly.
//@X
//@E // after reading teste_dataframe.csv as obj1
//@E std::vector&lt;unsigned int&gt; colv = {4, 1};
//@E obj1.rm_col(colv);
//@E obj1.display();
//@E     &lt;uint&gt; &lt;uint&gt; &lt;str&gt;  &lt;char&gt;
//@E     col1   col3   col4   col6
//@E :0:  1      3      aa     z
//@E :1:  6      8      bb     e
//@E :2:  1      3      cc     h
//@E :3:  6      8      uu     a
//@E :4:  1      3      s4     q
//@E :5:  6      8      s9     p
//@E :6:  1      3      a4     j
//@E :7:  6      8      m9     i
//@E :8:  6      8      s9     p
//@E :9:  1      3      a4     j
//@E :10: 6      8      m9     i
//@E :11: 6      8      m9     i
//@E :12: 6      8      s9     p
//@E :13: 1      3      NA     j
//@E :14: 6      8      m9     i
//@X

//@T Dataframe.rm_row
//@U void rm_col(std::vector&lt;unsigned int&gt; &nbcolv)
//@X
//@D Removes rows from associated dataframe.
//@A nbcolv : is a vector containing all the indices of the rows to erase from the associated dataframe. The indices must be sorted descendly.
//@X
//@E // after reading teste_dataframe.csv as obj1
//@E std::vector&lt;unsigned int&gt; rowv = {4, 1};
//@E obj1.rm_row(rowv);
//@E obj1.display();
//@E     &lt;uint&gt; &lt;uint&gt; &lt;uint&gt; &lt;str&gt; &lt;int&gt; &lt;char&gt;
//@E     col1   col2   col3   col4  col5  col6
//@E :0:  1      2      3      aa    5     z
//@E :1:  1      2      3      cc    5     h
//@E :2:  6      7      8      uu    10    a
//@E :3:  6      7      8      s9    10    p
//@E :4:  1      2      3      a4    5     j
//@E :5:  6      7      8      m9    10    i
//@E :6:  6      7      8      s9    10    p
//@E :7:  1      2      3      a4    5     j
//@E :8:  6      7      8      m9    10    i
//@E :9:  6      7      8      m9    10    i
//@E :10: 6      7      8      s9    10    p
//@E :11: 1      2      3      NA    5     j
//@E :12: 6      7      8      m9    10    i
//@X

//@T Dataframe.transform_inner
//@U void transform_inner(Dataframe &cur_obj, unsigned int &in_col, unsigned int &ext_col)
//@X
//@D Applies a inner join on the associated dataframe.
//@A cur_obj : is the other dataframe used for inner join
//@A in_col : is the index of the column representing the key (primary) of the associated dataframe
//@A ext_col : is the index of the column representing the key (foreign) of the other dataframe used for the inner join
//@X
//@E
//@E Dataframe obj1, obj2;
//@E std::string filename = "outb.csv";
//@E obj1.readf(filename);
//@E 
//@E std::vector&lt;unsigned int&gt; colv = {4, 3, 2};
//@E obj1.rm_col(colv);
//@E 
//@E std::string f2 = "outb2.csv";
//@E obj2.readf(f2);
//@E 
//@E unsigned int col1 = 0;
//@E unsigned int col2 = 0;
//@E 
//@E obj1.transform_inner(obj2, col1, col2);
//@E obj1.display();
//@E  &lt;str&gt; &lt;uint&gt;
//@E     col1  col2
//@E :0:  id1   1
//@E :1:  id2   6
//@E :2:  id4   6
//@E :3:  id5   1
//@E :4:  id6   6
//@E :5:  id7   1
//@E :6:  id8   6
//@E :7:  id9   6
//@E :8:  id11  6
//@E :9:  id12  6
//@E :10: id14  1
//@E :11: id15  6
//@X

//@T Dataframe.transform_inner_clean
//@U void transform_inner_clean(Dataframe &cur_obj, unsigned int &in_col, unsigned int &ext_col)
//@X
//@D Applies a inner join on the associated dataframe. Is basically the same as transform_inner but uses another algorithm that takes more time but frees memory after having kept the common elements.
//@A cur_obj : is the other dataframe used for inner join
//@A in_col : is the index of the column representing the key (primary) of the associated dataframe
//@A ext_col : is the index of the column representing the key (foreign) of the other dataframe used for the inner join
//@X
//@E
//@E Dataframe obj1, obj2;
//@E std::string filename = "outb.csv";
//@E obj1.readf(filename);
//@E 
//@E std::vector&lt;unsigned int&gt; colv = {4, 3, 2};
//@E obj1.rm_col(colv);
//@E 
//@E std::string f2 = "outb2.csv";
//@E obj2.readf(f2);
//@E 
//@E unsigned int col1 = 0;
//@E unsigned int col2 = 0;
//@E 
//@E obj1.transform_inner_clean(obj2, col1, col2);
//@E obj1.display();
//@E  &lt;str&gt; &lt;uint&gt;
//@E     col1  col2
//@E :0:  id1   1
//@E :1:  id2   6
//@E :2:  id4   6
//@E :3:  id5   1
//@E :4:  id6   6
//@E :5:  id7   1
//@E :6:  id8   6
//@E :7:  id9   6
//@E :8:  id11  6
//@E :9:  id12  6
//@E :10: id14  1
//@E :11: id15  6
//@X

//@T Dataframe.transform_excluding
//@U void transform_excluding(Dataframe &cur_obj, unsigned int &in_col, unsigned int &ext_col)
//@X
//@D Applies an excluding join on the associated dataframe.
//@A cur_obj : is the other dataframe used for inner join
//@A in_col : is the index of the column representing the key (primary) of the associated dataframe
//@A ext_col : is the index of the column representing the key (foreign) of the other dataframe used for the inner join
//@X
//@E
//@E Dataframe obj1, obj2;
//@E std::string filename = "outb.csv";
//@E obj1.readf(filename);
//@E 
//@E std::vector&lt;unsigned int&gt; colv = {4, 3, 2};
//@E obj1.rm_col(colv);
//@E 
//@E std::string f2 = "outb2.csv";
//@E obj2.readf(f2);
//@E 
//@E unsigned int col1 = 0;
//@E unsigned int col2 = 0;
//@E 
//@E obj1.transform_excluding(obj2, col1, col2);
//@E obj1.display();
//@E  &lt;str&gt; &lt;uint&gt;
//@E     col1  col2
//@E    col1  col2   col3   col4   col5
//@E :0: id3   1      2      3      cc
//@E :1: id10  1      2      3      a4
//@E :2: id13  6      7      8      s9
//@X

//@T Dataframe.transform_excluding_clean
//@U void transform_excluding_clean(Dataframe &cur_obj, unsigned int &in_col, unsigned int &ext_col)
//@X
//@D Applies an excluding join on the associated dataframe. Is basically the same as transform_excluding but uses another algorithm that takes more time but frees memory after having kept the common elements.
//@A cur_obj : is the other dataframe used for inner join
//@A in_col : is the index of the column representing the key (primary) of the associated dataframe
//@A ext_col : is the index of the column representing the key (foreign) of the other dataframe used for the inner join
//@X
//@E
//@E Dataframe obj1, obj2;
//@E std::string filename = "outb.csv";
//@E obj1.readf(filename);
//@E 
//@E std::vector&lt;unsigned int&gt; colv = {4, 3, 2};
//@E obj1.rm_col(colv);
//@E 
//@E std::string f2 = "outb2.csv";
//@E obj2.readf(f2);
//@E 
//@E unsigned int col1 = 0;
//@E unsigned int col2 = 0;
//@E 
//@E obj1.transform_excluding_clean(obj2, col1, col2);
//@E obj1.display();
//@E  &lt;str&gt; &lt;uint&gt;
//@E     col1  col2
//@E    col1  col2   col3   col4   col5
//@E :0: id3   1      2      3      cc
//@E :1: id10  1      2      3      a4
//@E :2: id13  6      7      8      s9
//@X

//@T Dataframe.merge_inner
//@U void merge_inner(Dataframe &obj1, Dataframe &obj2, bool colname, unsigned int &key1, unsigned int &key2)
//@X
//@D Performs a inner join on a newly created dataframe. May creates dupplicates if multiple occurences of the key is found in the second dataframes, use Dataframe.merge_inner2 if you do not want to create dupplicates.
//@A obj1 : is the first dataframe
//@A obj2 : is the second dataframe
//@A colname : 1 to give the column names to the newly created dataframe
//@A key1 : is the index of the first dataframe column as primary key
//@A key1 : is the index of the first dataframe column as foreign key
//@X
//@E 
//@E Dataframe obj1, obj2, obj3;
//@E std::string filename = "csv_test/outb.csv";
//@E obj1.readf(filename);
//@E 
//@E std::string f2 = "csv_test/outb2.csv";
//@E obj2.readf(f2);
//@E 
//@E unsigned int col1 = 0;
//@E unsigned int col2 = 0;
//@E 
//@E obj3.merge_inner(obj1, obj2, 1, col1, col2);
//@E obj3.display();
//@E     &lt;str&gt; &lt;uint&gt; &lt;uint&gt; &lt;uint&gt; &lt;str&gt; &lt;str&gt; &lt;uint&gt; &lt;uint&gt;
//@E     [0]   [1]    [2]    [3]    [4]   [5]   [6]    [7]
//@E :0:  id1   1      2      3      aa    id1   2      3
//@E :1:  id1   1      2      3      aa    id1   2      3
//@E :2:  id2   6      7      8      bb    id2   7      8
//@E :3:  id4   6      7      8      uu    id4   7      8
//@E :4:  id5   1      2      3      s4    id5   2      3
//@E :5:  id6   6      7      8      s9    id6   7      8
//@E :6:  id7   1      2      3      a4    id7   2      3
//@E :7:  id8   6      7      8      m9    id8   2      3
//@E :8:  id9   6      7      8      s9    id9   7      8
//@E :9:  id11  6      7      8      m9    id11  7      8
//@E :10: id11  6      7      8      m9    id11  7      8
//@E :11: id12  6      7      8      m9    id12  7      8
//@E :12: id14  1      2      3      NA    id14  7      8
//@E :13: id15  16     7      8      m9    id15  2      3
//@X

//@T Dataframe.merge_inner2
//@U void merge_inner2(Dataframe &obj1, Dataframe &obj2, bool colname, unsigned int &key1, unsigned int &key2)
//@X
//@D Performs a inner join on a newly created dataframe.
//@A obj1 : is the first dataframe
//@A obj2 : is the second dataframe
//@A colname : 1 to give the column names to the newly created dataframe
//@A key1 : is the index of the first dataframe column as primary key
//@A key1 : is the index of the first dataframe column as foreign key
//@X
//@E 
//@E Dataframe obj1, obj2, obj3;
//@E std::string filename = "csv_test/outb.csv";
//@E obj1.readf(filename);
//@E 
//@E std::string f2 = "csv_test/outb2.csv";
//@E obj2.readf(f2);
//@E 
//@E unsigned int col1 = 0;
//@E unsigned int col2 = 0;
//@E 
//@E obj3.merge_inner2(obj1, obj2, 1, col1, col2);
//@E obj3.display();
//@E     &lt;str&gt; &lt;uint&gt; &lt;uint&gt; &lt;uint&gt; &lt;str&gt; &lt;str&gt; &lt;uint&gt; &lt;uint&gt;
//@E     [0]   [1]    [2]    [3]    [4]   [5]   [6]    [7]
//@E :0:  id1   1      2      3      aa    id1   2      3
//@E :1:  id2   6      7      8      bb    id2   7      8
//@E :2:  id4   6      7      8      uu    id4   7      8
//@E :3:  id5   1      2      3      s4    id5   2      3
//@E :4:  id6   6      7      8      s9    id6   7      8
//@E :5:  id7   1      2      3      a4    id7   2      3
//@E :6:  id8   6      7      8      m9    id8   2      3
//@E :7:  id9   6      7      8      s9    id9   7      8
//@E :8:  id11  6      7      8      m9    id11  7      8
//@E :9:  id12  6      7      8      m9    id12  7      8
//@E :10: id14  1      2      3      NA    id14  7      8
//@E :11: id15  16     7      8      m9    id15  2      3
//@X

//@T Dataframe.merge_excluding
//@U void merge_excluding(Dataframe &obj1, 
//@U                          Dataframe &obj2, 
//@U                          bool colname, 
//@U                          unsigned int &key1, 
//@U                          unsigned int &key2,
//@U                          std::string default_str = "NA",
//@U                          std::string default_chr = " ",
//@U                          std::string default_bool = "0",
//@U                          std::string default_int = "0",
//@U                          std::string default_uint = "0",
//@U                          std::string default_dbl = "0")
//@X
//@D Performs a left excluding join to the associated dataframe (newly created). The first dataframe as argument is considered as the left one.
//@A obj1 : is the left dataframe
//@A obj2 : is the right dataframe
//@A colname : 1 to give the column names to the newly created dataframe
//@A key1 : is the index of the column of the left dataframe
//@A key2 : is the index of the column of the right dataframe
//@X
//@E Dataframe obj1, obj2, obj3;
//@E std::string filename = "csv_test/outb.csv";
//@E obj1.readf(filename);
//@E 
//@E std::string f2 = "csv_test/outb2.csv";
//@E obj2.readf(f2);
//@E 
//@E unsigned int col1 = 0;
//@E unsigned int col2 = 0;
//@E 
//@E obj3.merge_excluding(obj1, obj2, 1, col1, col2);
//@E obj3.display();
//@E    &lt;str&gt; &lt;uint&gt; &lt;uint&gt; &lt;uint&gt; &lt;str&gt; &lt;str&gt; &lt;uint&gt; &lt;uint&gt;
//@E    [0]   [1]    [2]    [3]    [4]   [5]   [6]    [7]
//@E :0: id3   1      2      3      cc    NA    0      0
//@E :1: id10  1      2      3      a4    NA    0      0
//@E :2: id13  6      7      8      s9    NA    0      0
//@X

//@T Dataframe.merge_excluding_both
//@U void merge_excluding_both(Dataframe &obj1, 
//@U                               Dataframe &obj2, 
//@U                               bool colname, 
//@U                               unsigned int &key1, 
//@U                               unsigned int &key2,
//@U                               std::string default_str = "NA",
//@U                               std::string default_chr = " ",
//@U                               std::string default_bool = "0",
//@U                               std::string default_int = "0",
//@U                               std::string default_uint = "0",
//@U                               std::string default_dbl = "0")
//@X
//@D Performs a full excluding join to the associated dataframe (newly created). The first dataframe as argument is considered as the left one.
//@A obj1 : is the left dataframe
//@A obj2 : is the right dataframe
//@A colname : 1 to give the column names to the newly created dataframe
//@A key1 : is the index of the column of the left dataframe
//@A key2 : is the index of the column of the right dataframe
//@X
//@E Dataframe obj1, obj2, obj3;
//@E std::string filename = "csv_test/outb.csv";
//@E obj1.readf(filename);
//@E 
//@E std::string f2 = "csv_test/outb2.csv";
//@E obj2.readf(f2);
//@E 
//@E unsigned int col1 = 0;
//@E unsigned int col2 = 0;
//@E 
//@E obj3.merge_excluding_both(obj1, obj2, 1, col1, col2);
//@E obj3.display();
//@E    &lt;str&gt; &lt;uint&gt; &lt;uint&gt; &lt;uint&gt; &lt;str&gt; &lt;str&gt; &lt;uint&gt; &lt;uint&gt;
//@E    [0]   [1]    [2]    [3]    [4]   [5]   [6]    [7]
//@E :0: id3   1      2      3      cc    NA    0      0
//@E :1: id10  1      2      3      a4    NA    0      0
//@E :2: id13  6      7      8      s9    NA    0      0
//@E :3: NA    0      0      0      NA    id119 7      8
//@X

//@T Dataframe.merge_all
//@U void merge_all(Dataframe &obj1, 
//@U                     Dataframe &obj2, 
//@U                     bool colname, 
//@U                     unsigned int &key1, 
//@U                     unsigned int &key2,
//@U                     std::string default_str = "NA",
//@U                     std::string default_chr = " ",
//@U                     std::string default_bool = "0",
//@U                     std::string default_int = "0",
//@U                     std::string default_uint = "0",
//@U                     std::string default_dbl = "0") 
//@X
//@D Performs a full join to the associated dataframe (newly created). The first dataframe as argument is considered as the left one. May producesdupplicates if several occurences of the same key appears in the second dataframe, use Dataframe.merge_all2 if you do not want to create dupplicates.
//@A obj1 : is the left dataframe
//@A obj2 : is the right dataframe
//@A colname : 1 to give the column names to the newly created dataframe
//@A key1 : is the index of the column of the left dataframe
//@A key2 : is the index of the column of the right dataframe
//@X
//@E Dataframe obj1, obj2, obj3;
//@E std::string filename = "csv_test/outb.csv";
//@E obj1.readf(filename);
//@E 
//@E std::string f2 = "csv_test/outb2.csv";
//@E obj2.readf(f2);
//@E 
//@E unsigned int col1 = 0;
//@E unsigned int col2 = 0;
//@E 
//@E obj3.merge_all(obj1, obj2, 1, col1, col2);
//@E obj3.display();
//@E     &lt;str&gt; &lt;uint&gt; &lt;uint&gt; &lt;uint&gt; &lt;str&gt; &lt;str&gt; &lt;uint&gt; &lt;uint&gt;
//@E     [0]   [1]    [2]    [3]    [4]   [5]   [6]    [7]
//@E :0:  id1   1      2      3      aa    id1   2      3
//@E :1:  id1   1      2      3      aa    id1   22     3
//@E :2:  id2   6      7      8      bb    id2   7      8
//@E :3:  id3   1      2      3      cc    NA    0      0
//@E :4:  id4   6      7      8      uu    id4   7      8
//@E :5:  id5   1      2      3      s4    id5   2      3
//@E :6:  id6   6      7      8      s9    id6   7      8
//@E :7:  id7   1      2      3      a4    id7   2      3
//@E :8:  id8   6      7      8      m9    id8   2      3
//@E :9:  id9   6      7      8      s9    id9   7      8
//@E :10: id10  1      2      3      a4    NA    0      0
//@E :11: id11  6      7      8      m9    id11  7      8
//@E :12: id11  6      7      8      m9    id11  17     8
//@E :13: id12  6      7      8      m9    id12  7      8
//@E :14: id13  6      7      8      s9    NA    0      0
//@E :15: id14  1      2      3      NA    id14  7      8
//@E :16: id15  6      7      8      m9    id15  2      3
//@E :17: NA    NA     NA     NA     NA    id119 7      8
//@X

//@T Dataframe.merge_all2
//@U void merge_all2(Dataframe &obj1, 
//@U                     Dataframe &obj2, 
//@U                     bool colname, 
//@U                     unsigned int &key1, 
//@U                     unsigned int &key2,
//@U                     std::string default_str = "NA",
//@U                     std::string default_chr = " ",
//@U                     std::string default_bool = "0",
//@U                     std::string default_int = "0",
//@U                     std::string default_uint = "0",
//@U                     std::string default_dbl = "0") 
//@X
//@D Performs a full join to the associated dataframe (newly created). The first dataframe as argument is considered as the left one.
//@A obj1 : is the left dataframe
//@A obj2 : is the right dataframe
//@A colname : 1 to give the column names to the newly created dataframe
//@A key1 : is the index of the column of the left dataframe
//@A key2 : is the index of the column of the right dataframe
//@X
//@E Dataframe obj1, obj2, obj3;
//@E std::string filename = "csv_test/outb.csv";
//@E obj1.readf(filename);
//@E 
//@E std::string f2 = "csv_test/outb2.csv";
//@E obj2.readf(f2);
//@E 
//@E unsigned int col1 = 0;
//@E unsigned int col2 = 0;
//@E 
//@E obj3.merge_all2(obj1, obj2, 1, col1, col2);
//@E obj3.display();
//@E     &lt;str&gt; &lt;uint&gt; &lt;uint&gt; &lt;uint&gt; &lt;str&gt; &lt;str&gt; &lt;uint&gt; &lt;uint&gt;
//@E     [0]   [1]    [2]    [3]    [4]   [5]   [6]    [7]
//@E :0:  id1   1      2      3      aa    id1   2      3
//@E :1:  id2   6      7      8      bb    id2   7      8
//@E :2:  id3   1      2      3      cc    NA    0      0
//@E :3:  id4   6      7      8      uu    id4   7      8
//@E :4:  id5   1      2      3      s4    id5   2      3
//@E :5:  id6   6      7      8      s9    id6   7      8
//@E :6:  id7   1      2      3      a4    id7   2      3
//@E :7:  id8   6      7      8      m9    id8   2      3
//@E :8:  id9   6      7      8      s9    id9   7      8
//@E :9:  id10  1      2      3      a4    NA    0      0
//@E :10: id11  6      7      8      m9    id11  7      8
//@E :11: id12  6      7      8      m9    id12  7      8
//@E :12: id13  6      7      8      s9    NA    0      0
//@E :13: id14  1      2      3      NA    id14  7      8
//@E :14: id15  16     7      8      m9    id15  2      3
//@E :15: NA    0      0      0      NA    id119 7      8
//@X

//@T Dataframe.transform_merge_inner2
//@U void transform_merge_inner2(Dataframe &obj1, Dataframe &obj2, unsigned int &key1, unsigned int &key2)
//@X
//@D Performs a inner join on a the current dataframe with another one.
//@A obj1 : is the second dataframe
//@A key1 : is the index of the first dataframe column as primary key
//@A key1 : is the index of the first dataframe column as foreign key
//@X
//@E 
//@E  Dataframe obj1, obj2;
//@E  std::string fname = "csv_test/outb.csv";
//@E  obj1.readf(fname);
//@E
//@E  std::cout << "\n";
//@E
//@E  fname = "csv_test/outb2.csv";
//@E  obj2.readf(fname);
//@E
//@E  unsigned int n = 0;
//@E  std::cout << "\n";
//@E
//@E  obj1.transform_merge_inner2(obj2, n, n);
//@E  obj1.display();
//@E     &lt;str&gt; &lt;uint&gt; &lt;uint&gt; &lt;uint&gt; &lt;str&gt; &lt;str&gt; &lt;uint&gt; &lt;uint&gt;
//@E     col1  col2   col3   col4   col5  col1  col2   col3
//@E :0:  id1   1      2      3      aa    id1   2      3
//@E :1:  id2   6      7      8      bb    id2   7      8
//@E :2:  id4   6      7      8      uu    id4   7      8
//@E :3:  id5   1      2      3      s4    id5   2      3
//@E :4:  id6   6      7      8      s9    id6   7      8
//@E :5:  id7   1      2      3      a4    id7   2      3
//@E :6:  id8   6      7      8      m9    id8   2      3
//@E :7:  id9   6      7      8      s9    id9   7      8
//@E :8:  id11  6      7      8      m9    id11  7      8
//@E :9:  id12  6      7      8      m9    id12  7      8
//@E :10: id14  1      2      3      NA    id14  7      8
//@E :11: id15  16     7      8      m9    id15  2      3
//@X

//@T Dataframe.transform_merge_inner2_clean
//@U void transform_merge_inner2_clean(Dataframe &obj1, Dataframe &obj2, unsigned int &key1, unsigned int &key2)
//@X
//@D Performs a inner join on a the current dataframe with another one. Potentially significantly slower than transform_merge_inner2 because it frees unnused memory.
//@A obj1 : is the second dataframe
//@A key1 : is the index of the first dataframe column as primary key
//@A key1 : is the index of the first dataframe column as foreign key
//@X
//@E 
//@E  Dataframe obj1, obj2;
//@E  std::string fname = "csv_test/outb.csv";
//@E  obj1.readf(fname);
//@E
//@E  std::cout << "\n";
//@E
//@E  fname = "csv_test/outb2.csv";
//@E  obj2.readf(fname);
//@E
//@E  unsigned int n = 0;
//@E  std::cout << "\n";
//@E
//@E  obj1.transform_merge_inner2_clean(obj2, n, n);
//@E  obj1.display();
//@E     &lt;str&gt; &lt;uint&gt; &lt;uint&gt; &lt;uint&gt; &lt;str&gt; &lt;str&gt; &lt;uint&gt; &lt;uint&gt;
//@E     col1  col2   col3   col4   col5  col1  col2   col3
//@E :0:  id1   1      2      3      aa    id1   2      3
//@E :1:  id2   6      7      8      bb    id2   7      8
//@E :2:  id4   6      7      8      uu    id4   7      8
//@E :3:  id5   1      2      3      s4    id5   2      3
//@E :4:  id6   6      7      8      s9    id6   7      8
//@E :5:  id7   1      2      3      a4    id7   2      3
//@E :6:  id8   6      7      8      m9    id8   2      3
//@E :7:  id9   6      7      8      s9    id9   7      8
//@E :8:  id11  6      7      8      m9    id11  7      8
//@E :9:  id12  6      7      8      m9    id12  7      8
//@E :10: id14  1      2      3      NA    id14  7      8
//@E :11: id15  16     7      8      m9    id15  2      3
//@X

//@T Dataframe.transform_left_join
//@U void transform_left_join(Dataframe &obj, 
//@U                     unsigned int &key1, 
//@U                     unsigned int &key2,
//@U                     std::string default_str = "NA",
//@U                     char default_chr = ' ',
//@U                     bool default_bool = 0,
//@U                     int default_int = 0,
//@U                     unsigned int default_uint = 0,
//@U                     double default_dbl = 0) 
//@X
//@D Transforms the dataframe performing a left join.
//@A obj : is the second dataframe
//@A key1 : is the column index of the primary key
//@A key2 : is the foreign key
//@A default_str : is the default value for NA string values
//@A default_chr : is the default value for NA char values
//@A default_bool : is the default value for NA boolean values
//@A default_int : is the default value for NA int values
//@A default_uint : is the default value for NA unsigned int values
//@A default_dbl : is the default value for NA double values
//@X
//@E  Dataframe obj1, obj2;
//@E  std::string fname = "csv_test/outb.csv";
//@E  obj1.readf(fname);
//@E  fname = "csv_test/outb2.csv";
//@E  obj2.readf(fname);
//@E
//@E  unsigned int n = 0;
//@E  obj1.transform_left_join(obj2, n, n);
//@E
//@E  obj1.display();
//@E     &lt;str&gt; &lt;uint&gt; &lt;uint&gt; &lt;uint&gt; &lt;str&gt; &lt;str&gt; &lt;uint&gt; &lt;uint&gt;
//@E     col1  col2   col3   col4   col5  col1  col2   col3
//@E :0:  id1   1      2      3      aa    id1   2      3
//@E :1:  id2   6      7      8      bb    id2   7      8
//@E :2:  id3   1      2      3      cc    NA    0      0
//@E :3:  id4   6      7      8      uu    id4   7      8
//@E :4:  id5   1      2      3      s4    id5   2      3
//@E :5:  id6   6      7      8      s9    id6   7      8
//@E :6:  id7   1      2      3      a4    id7   2      3
//@E :7:  id8   6      7      8      m9    id8   2      3
//@E :8:  id9   6      7      8      s9    id9   7      8
//@E :9:  id10  1      2      3      a4    NA    0      0
//@E :10: id11  6      7      8      m9    id11  7      8
//@E :11: id12  6      7      8      m9    id12  7      8
//@E :12: id13  6      7      8      s9    NA    0      0
//@E :13: id14  1      2      3      NA    id14  7      8
//@E :14: id15  16     7      8      m9    id15  2      3
//@X

//@T Dataframe.transform_filter
//@U void transform_filter(std::vector&lt;bool&gt;& mask)
//@X
//@D Keeps the desired row from a boolean mask.
//@A mask : is the boolean mask
//@X
//@E Dataframe obj1, obj2;
//@E std::string fname = "csv_test/outb.csv";
//@E obj1.readf(fname);
//@E fname = "csv_test/outb2.csv";
//@E obj2.readf(fname);
//@E
//@E std::vector<bool> mask = {0, 1, 0, 1, 1, 1, 0, 1, 0, 1, 1, 1, 0, 1, 0, 1};
//@E
//@E obj1.transform_filter(mask);
//@E
//@E obj1.display();
//@E    &lt;str&gt; &lt;uint&gt; &lt;uint&gt; &lt;uint&gt; &lt;str&gt;
//@E    col1  col2   col3   col4   col5
//@E :0: id2   6      7      8      bb
//@E :1: id4   6      7      8      uu
//@E :2: id5   1      2      3      s4
//@E :3: id6   6      7      8      s9
//@E :4: id8   6      7      8      m9
//@E :5: id10  1      2      3      a4
//@E :6: id11  6      7      8      m9
//@E :7: id12  6      7      8      m9
//@E :8: id14  1      2      3      NA
//@X

//@T Dataframe.pivot_dbl
//@U void pivot_dbl(Dataframe &obj, unsigned int &n1, unsigned int& n2, unsigned int& n3)
//@X
//@D Performs a pivot to a newly created dataframe.
//@A obj : is the dataframe from which the pivot is performed
//@A n1 : is the column index of the columns created for the pivot
//@A n2 : is the column index of the rows created for the pivot
//@A n3 : is the column index of the column from which the pivot is performed
//@X
//@E
//@E  // after some random manipulation for obj1 dataframe
//@E  Dataframe obj3;
//@E  obj1.display();
//@E
//@E     &lt;str&gt; &lt;uint&gt; &lt;uint&gt; &lt;uint&gt; &lt;str&gt; &lt;str&gt; &lt;uint&gt; &lt;double&gt;
//@E     col1  col2   col3   col4   col5  col1  col2   col3
//@E :0:  id1   1      2      3      A     D     2      3
//@E :1:  id2   6      7      8      A     D     7      8
//@E :2:  id3   1      2      3      A     C     0      0
//@E :3:  id4   6      7      8      B     C     7      8
//@E :4:  id5   1      2      3      B     C     2      3
//@E :5:  id6   6      7      8      A     D     7      8
//@E :6:  id7   1      2      3      B     C     2      3
//@E :7:  id8   6      7      8      B     D     2      3
//@E :8:  id9   6      7      8      B     D     7      8
//@E :9:  id10  1      2      3      A     C     0      0
//@E :10: id11  6      7      8      A     D     7      8
//@E :11: id12  6      7      8      B     C     7      8
//@E :12: id13  6      7      8      B     D     0      0
//@E :13: id14  1      2      3      B     C     7      8
//@E :14: id15  16     7      8      B     D     2      3
//@E
//@E  unsigned int n1 = 4;
//@E  unsigned int n2 = 5;
//@E  unsigned int n3 = 7;
//@E
//@E  obj3.pivot_uint(obj1, n, n2, n3);
//@E
//@E  obj3.display();
//@E
//@E    &lt;double&gt; &lt;double&gt;
//@E    A        B
//@E D : 27       14
//@E C : 0        30
//@E
//@X

//@T Dataframe.pivot_int
//@U void pivot_int(Dataframe &obj, unsigned int &n1, unsigned int& n2, unsigned int& n3)
//@X
//@D Performs a pivot to a newly created dataframe.
//@A obj : is the dataframe from which the pivot is performed
//@A n1 : is the column index of the columns created for the pivot
//@A n2 : is the column index of the rows created for the pivot
//@A n3 : is the column index of the column from which the pivot is performed
//@X
//@E
//@E  // after some random manipulation for obj1 dataframe
//@E  Dataframe obj3;
//@E  obj1.display();
//@E
//@E     &lt;str&gt; &lt;uint&gt; &lt;uint&gt; &lt;uint&gt; &lt;str&gt; &lt;str&gt; &lt;uint&gt; &lt;int&gt;
//@E     col1  col2   col3   col4   col5  col1  col2   col3
//@E :0:  id1   1      2      3      A     D     2      3
//@E :1:  id2   6      7      8      A     D     7      8
//@E :2:  id3   1      2      3      A     C     0      0
//@E :3:  id4   6      7      8      B     C     7      8
//@E :4:  id5   1      2      3      B     C     2      3
//@E :5:  id6   6      7      8      A     D     7      8
//@E :6:  id7   1      2      3      B     C     2      3
//@E :7:  id8   6      7      8      B     D     2      3
//@E :8:  id9   6      7      8      B     D     7      8
//@E :9:  id10  1      2      3      A     C     0      0
//@E :10: id11  6      7      8      A     D     7      8
//@E :11: id12  6      7      8      B     C     7      8
//@E :12: id13  6      7      8      B     D     0      0
//@E :13: id14  1      2      3      B     C     7      8
//@E :14: id15  16     7      8      B     D     2      3
//@E
//@E  unsigned int n1 = 4;
//@E  unsigned int n2 = 5;
//@E  unsigned int n3 = 7;
//@E
//@E  obj3.pivot_uint(obj1, n, n2, n3);
//@E
//@E  obj3.display();
//@E
//@E    &lt;int&gt; &lt;int&gt;
//@E    A        B
//@E D : 27       14
//@E C : 0        30
//@E
//@X

//@T Dataframe.pivot_uint
//@U void pivot_uint(Dataframe &obj, unsigned int &n1, unsigned int& n2, unsigned int& n3)
//@X
//@D Performs a pivot to a newly created dataframe.
//@A obj : is the dataframe from which the pivot is performed
//@A n1 : is the column index of the columns created for the pivot
//@A n2 : is the column index of the rows created for the pivot
//@A n3 : is the column index of the column from which the pivot is performed
//@X
//@E
//@E  // after some random manipulation for obj1 dataframe
//@E  Dataframe obj3;
//@E  obj1.display();
//@E
//@E     &lt;str&gt; &lt;uint&gt; &lt;uint&gt; &lt;uint&gt; &lt;str&gt; &lt;str&gt; &lt;uint&gt; &lt;uint&gt;
//@E     col1  col2   col3   col4   col5  col1  col2   col3
//@E :0:  id1   1      2      3      A     D     2      3
//@E :1:  id2   6      7      8      A     D     7      8
//@E :2:  id3   1      2      3      A     C     0      0
//@E :3:  id4   6      7      8      B     C     7      8
//@E :4:  id5   1      2      3      B     C     2      3
//@E :5:  id6   6      7      8      A     D     7      8
//@E :6:  id7   1      2      3      B     C     2      3
//@E :7:  id8   6      7      8      B     D     2      3
//@E :8:  id9   6      7      8      B     D     7      8
//@E :9:  id10  1      2      3      A     C     0      0
//@E :10: id11  6      7      8      A     D     7      8
//@E :11: id12  6      7      8      B     C     7      8
//@E :12: id13  6      7      8      B     D     0      0
//@E :13: id14  1      2      3      B     C     7      8
//@E :14: id15  16     7      8      B     D     2      3
//@E
//@E  unsigned int n1 = 4;
//@E  unsigned int n2 = 5;
//@E  unsigned int n3 = 7;
//@E
//@E  obj3.pivot_uint(obj1, n, n2, n3);
//@E
//@E  obj3.display();
//@E
//@E    &lt;uint&gt; &lt;uint&gt;
//@E    A        B
//@E D : 27       14
//@E C : 0        30
//@E
//@X

//@T Dataframe.get_typecol
//@U const std::vector&lt;char&gt;& get_typecol() const
//@X
//@D Returns the column type of the dataframe, 'i' (int), 'u', (unsigned int), 'd', double, 's' (string), 'c' (char)
//@A X : NO ARGS
//@X
//@E const std::vector<char>& dtype = obj1.get_typecol();
//@E 'c' 'b' 'd' 'i' 
//@X

//@T Dataframe.transform_unique
//@U void transform_unique(unsigned int& n)
//@X
//@D Trnasforms your current dataframe to keep the unique values from a chosen column.
//@A n : is the column index
//@X
//@E   std::string fname = "csv_test/outb.csv";
//@E   obj1.readf&lt;3, 0&gt;(fname);
//@E   obj1.display();
//@E   
//@E     &lt;str&gt; &lt;uint&gt; &lt;uint&gt; &lt;uint&gt; &lt;str&gt;
//@E     col1  col2   col3   col4   col5
//@E :0:  id4   6      7      8      uu
//@E :1:  id5   1      2      3      s4
//@E :2:  id6   6      7      8      s9
//@E :3:  id7   1      2      3      a4
//@E :4:  id8   6      7      8      m9
//@E :5:  id9   6      7      8      s9
//@E :6:  id10  1      2      3      a4
//@E :7:  id11  6      7      8      m9
//@E :8:  id12  6      7      8      m9
//@E :9:  id13  6      7      8      s9
//@E :10: id14  1      2      3      NA
//@E :11: id15  16     7      8      m9
//@E
//@E   unsigned int n = 4;
//@E   obj1.transform_unique(n);
//@E   obj1.display();
//@E
//@E    &lt;str&gt; &lt;uint&gt; &lt;uint&gt; &lt;uint&gt; &lt;str&gt;
//@E    col1  col2   col3   col4   col5
//@E :0: id4   6      7      8      uu
//@E :1: id5   1      2      3      s4
//@E :2: id6   6      7      8      s9
//@E :3: id7   1      2      3      a4
//@E :4: id8   6      7      8      m9
//@E :5: id14  1      2      3      NA
//@E
//@X

//@T Dataframe.transform_unique_clean
//@U void transform_unique_clean(unsigned int& n)
//@X
//@D Trnasforms your current dataframe to keep the unique values from a chosen column. Basically the same as transfrm_unique, but frees memory, which can be slower.
//@A n : is the column index
//@X
//@E   std::string fname = "csv_test/outb.csv";
//@E   obj1.readf&lt;3, 0&gt;(fname);
//@E   obj1.display();
//@E   
//@E     &lt;str&gt; &lt;uint&gt; &lt;uint&gt; &lt;uint&gt; &lt;str&gt;
//@E     col1  col2   col3   col4   col5
//@E :0:  id4   6      7      8      uu
//@E :1:  id5   1      2      3      s4
//@E :2:  id6   6      7      8      s9
//@E :3:  id7   1      2      3      a4
//@E :4:  id8   6      7      8      m9
//@E :5:  id9   6      7      8      s9
//@E :6:  id10  1      2      3      a4
//@E :7:  id11  6      7      8      m9
//@E :8:  id12  6      7      8      m9
//@E :9:  id13  6      7      8      s9
//@E :10: id14  1      2      3      NA
//@E :11: id15  16     7      8      m9
//@E
//@E   unsigned int n = 4;
//@E   obj1.transform_unique_clean(n);
//@E   obj1.display();
//@E
//@E    &lt;str&gt; &lt;uint&gt; &lt;uint&gt; &lt;uint&gt; &lt;str&gt;
//@E    col1  col2   col3   col4   col5
//@E :0: id4   6      7      8      uu
//@E :1: id5   1      2      3      s4
//@E :2: id6   6      7      8      s9
//@E :3: id7   1      2      3      a4
//@E :4: id8   6      7      8      m9
//@E :5: id14  1      2      3      NA
//@E
//@X

//@T Dataframe.transform_merge_excluding
//@U void transform_merge_excluding(Dataframe &obj, 
//@U                          unsigned int &key1, 
//@U                          unsigned int &key2,
//@U                          std::string default_str = "NA",
//@U                          char default_chr = ' ',
//@U                          bool default_bool = 0,
//@U                          int default_int = 0,
//@U                          unsigned int default_uint = 0,
//@U                          double default_dbl = 0)
//@X
//@D Performs a left excluding join to the associated dataframe (already created). The dataframe as argument is considered as the right one.
//@A obj : is the left dataframe
//@A colname : 1 to give the column names to the newly created dataframe
//@A key1 : is the index of the column of the left dataframe
//@A key2 : is the index of the column of the right dataframe
//@X
//@E
//@E  Dataframe obj1, obj2, obj3;
//@E  std::string fname = "csv_test/outb.csv";
//@E  obj1.readf(fname);
//@E
//@E  fname = "csv_test/outb2.csv";
//@E  obj2.readf(fname);
//@E
//@E  unsigned int n = 0;
//@E
//@E  obj1.transform_merge_excluding(obj2, n, n);
//@E  obj1.display();
//@E    &lt;str&gt; &lt;uint&gt; &lt;uint&gt; &lt;uint&gt; &lt;str&gt; &lt;str&gt; &lt;uint&gt; &lt;uint&gt;
//@E    col1  col2   col3   col4   col5  col1  col2   col3
//@E :0: id3   1      2      3      cc    NA    0      0
//@E :1: id10  1      2      3      a4    NA    0      0
//@E :2: id13  6      7      8      s9    NA    0      0
//@X

//@T Dataframe.transform_merge_excluding_clean
//@U void transform_merge_excluding_clean(Dataframe &obj, 
//@U                          unsigned int &key1, 
//@U                          unsigned int &key2,
//@U                          std::string default_str = "NA",
//@U                          char default_chr = ' ',
//@U                          bool default_bool = 0,
//@U                          int default_int = 0,
//@U                          unsigned int default_uint = 0,
//@U                          double default_dbl = 0)
//@X
//@D Performs a left excluding join to the associated dataframe (already created). The dataframe as argument is considered as the right one. May be significantly slower than transform_merge_excluding because this version frees unnused memory after doing the left excluding join.
//@A obj : is the left dataframe
//@A colname : 1 to give the column names to the newly created dataframe
//@A key1 : is the index of the column of the left dataframe
//@A key2 : is the index of the column of the right dataframe
//@X
//@E
//@E  Dataframe obj1, obj2, obj3;
//@E  std::string fname = "csv_test/outb.csv";
//@E  obj1.readf(fname);
//@E
//@E  fname = "csv_test/outb2.csv";
//@E  obj2.readf(fname);
//@E
//@E  unsigned int n = 0;
//@E
//@E  obj1.transform_merge_excluding_clean(obj2, n, n);
//@E  obj1.display();
//@E    &lt;str&gt; &lt;uint&gt; &lt;uint&gt; &lt;uint&gt; &lt;str&gt; &lt;str&gt; &lt;uint&gt; &lt;uint&gt;
//@E    col1  col2   col3   col4   col5  col1  col2   col3
//@E :0: id3   1      2      3      cc    NA    0      0
//@E :1: id10  1      2      3      a4    NA    0      0
//@E :2: id13  6      7      8      s9    NA    0      0
//@X

//@T Dataframe.sort_by
//@U template &lt;bool asc = 1&gt;
//@U void sort_by(unsigned int& n)
//@X
//@D Transforms the dataframe by reordering the rows by the value of a column.
//@A n : is the column index of the column value to reorder from
//@X
//@E
//@E  Dataframe obj1;
//@E  std::string fname = "csv_test/outb.csv";
//@E  obj1.readf(fname);
//@E  obj1.display();
//@E
//@E  obj1.display();
//@E 
//@E     &lt;str&gt; &lt;uint&gt; &lt;uint&gt; &lt;uint&gt; &lt;str&gt;
//@E     col1  col2   col3   col4   col5
//@E :0:  id1   1      2      3      aa
//@E :1:  id2   6      7      8      bb
//@E :2:  id3   1      2      3      cc
//@E :3:  id4   6      7      8      uu
//@E :4:  id5   1      2      3      s4
//@E :5:  id6   6      7      8      s9
//@E :6:  id7   1      2      3      a4
//@E :7:  id8   6      7      8      m9
//@E :8:  id9   6      7      8      s9
//@E :9:  id10  1      2      3      a4
//@E :10: id11  6      7      8      m9
//@E :11: id12  6      7      8      m9
//@E :12: id13  6      7      8      s9
//@E :13: id14  1      2      3      NA
//@E :14: id15  16     7      8      m9
//@E 
//@E  n = 2;
//@E  obj1.sort_by(n);
//@E
//@E  obj1.display();
//@E
//@E     &lt;str&gt; &lt;uint&gt; &lt;uint&gt; &lt;uint&gt; &lt;str&gt;
//@E     col1  col2   col3   col4   col5
//@E :0:  id1   1      2      3      aa
//@E :1:  id3   1      2      3      cc
//@E :2:  id5   1      2      3      s4
//@E :3:  id7   1      2      3      a4
//@E :4:  id10  1      2      3      a4
//@E :5:  id14  1      2      3      NA
//@E :6:  id2   6      7      8      bb
//@E :7:  id4   6      7      8      uu
//@E :8:  id6   6      7      8      s9
//@E :9:  id8   6      7      8      m9
//@E :10: id9   6      7      8      s9
//@E :11: id11  6      7      8      m9
//@E :12: id12  6      7      8      m9
//@E :13: id13  6      7      8      s9
//@E :14: id15  16     7      8      m9
//@E
//@E  obj1.sort_by<0>(n);
//@E
//@E  obj1.display();
//@E
//@E     &lt;str&gt; &lt;uint&gt; &lt;uint&gt; &lt;uint&gt; &lt;str&gt;
//@E     col1  col2   col3   col4   col5
//@E :0:  id2   6      7      8      bb
//@E :1:  id4   6      7      8      uu
//@E :2:  id6   6      7      8      s9
//@E :3:  id8   6      7      8      m9
//@E :4:  id9   6      7      8      s9
//@E :5:  id11  6      7      8      m9
//@E :6:  id12  6      7      8      m9
//@E :7:  id13  6      7      8      s9
//@E :8:  id15  16     7      8      m9
//@E :9:  id1   1      2      3      aa
//@E :10: id3   1      2      3      cc
//@E :11: id5   1      2      3      s4
//@E :12: id7   1      2      3      a4
//@E :13: id10  1      2      3      a4
//@E :14: id14  1      2      3      NA
//@E
//@X

//@T Dataframe.concat
//@U void concat(Dataframe& obj)
//@X
//@D Concaenates the row of the dataframes. See example.
//@A obj : is the input datarame (from which the rows will be appended)
//@X
//@E
//@E  Dataframe obj1, obj2, obj3;
//@E  std::string fname = "csv_test/outb.csv";
//@E  obj1.readf(fname);
//@E  obj1.display();
//@E
//@E     &lt;str&gt; &lt;uint&gt; &lt;uint&gt; &lt;uint&gt; &lt;str&gt;
//@E     col1  col2   col3   col4   col5
//@E :0:  id1   1      2      3      aa
//@E :1:  id2   6      7      8      bb
//@E :2:  id3   1      2      3      cc
//@E :3:  id4   6      7      8      uu
//@E :4:  id5   1      2      3      s4
//@E :5:  id6   6      7      8      s9
//@E :6:  id7   1      2      3      a4
//@E :7:  id8   6      7      8      m9
//@E :8:  id9   6      7      8      s9
//@E :9:  id10  1      2      3      a4
//@E :10: id11  6      7      8      m9
//@E :11: id12  6      7      8      m9
//@E :12: id13  6      7      8      s9
//@E :13: id14  1      2      3      NA
//@E :14: id15  16     7      8      m9
//@E
//@E  fname = "csv_test/outb.csv";
//@E  obj2.readf(fname);
//@E
//@E  obj1.concat(obj2);
//@E
//@E  obj1.display();
//@E
//@E     &lt;str&gt; &lt;uint&gt; &lt;uint&gt; &lt;uint&gt; &lt;str&gt;
//@E     col1  col2   col3   col4   col5
//@E :0:  id1   1      2      3      aa
//@E :1:  id2   6      7      8      bb
//@E :2:  id3   1      2      3      cc
//@E :3:  id4   6      7      8      uu
//@E :4:  id5   1      2      3      s4
//@E :5:  id6   6      7      8      s9
//@E :6:  id7   1      2      3      a4
//@E :7:  id8   6      7      8      m9
//@E :8:  id9   6      7      8      s9
//@E :9:  id10  1      2      3      a4
//@E :10: id11  6      7      8      m9
//@E :11: id12  6      7      8      m9
//@E :12: id13  6      7      8      s9
//@E :13: id14  1      2      3      NA
//@E :14: id15  16     7      8      m9
//@E :15: id1   1      2      3      aa
//@E :16: id2   6      7      8      bb
//@E :17: id3   1      2      3      cc
//@E :18: id4   6      7      8      uu
//@E :19: id5   1      2      3      s4
//@E :20: id6   6      7      8      s9
//@E :21: id7   1      2      3      a4
//@E :22: id8   6      7      8      m9
//@E :23: id9   6      7      8      s9
//@E :24: id10  1      2      3      a4
//@E :25: id11  6      7      8      m9
//@E :26: id12  6      7      8      m9
//@E :27: id13  6      7      8      s9
//@E :28: id14  1      2      3      NA
//@E :29: id15  16     7      8      m9
//@E
//@X
#endif


