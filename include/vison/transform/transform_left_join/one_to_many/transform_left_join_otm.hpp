#pragma once

template <bool SimdHash = true>
void transform_left_join_otm(Dataframe &obj_l,
                             Dataframe &obj_r,
                             const unsigned int &key1, 
                             const unsigned int &key2,
                             const std::string default_str = "NA",
                             const char default_chr = ' ',
                             const bool default_bool = 0,
                             const int default_int = 0,
                             const unsigned int default_uint = 0,
                             const double default_dbl = 0) 
{
  
    const unsigned int& ncol1 = obj_l.get_ncol();
    const unsigned int& ncol2 = obj_r.get_ncol();

    const unsigned int& nrow1 = obj_l.get_nrow();
    const unsigned int& nrow2 = obj_r.get_nrow();

    const std::vector<std::vector<unsigned int>>& matr_idx1 = obj_l.get_matr_idx();
    const std::vector<std::vector<unsigned int>>& matr_idx2 = obj_r.get_matr_idx();

    const std::vector<std::string>& str_v2   = obj_r.get_str_vec();
    const std::vector<char>& chr_v2          = obj_r.get_chr_vec();
    const std::vector<bool>& bool_v2         = obj_r.get_bool_vec();
    const std::vector<int>& int_v2           = obj_r.get_int_vec();
    const std::vector<unsigned int>& uint_v2 = obj_r.get_uint_vec();
    const std::vector<double>& dbl_v2        = obj_r.get_dbl_vec();
 
    const std::vector<std::string>& str_v1   = obj_l.get_str_vec();
    const std::vector<char>& chr_v1          = obj_l.get_chr_vec();
    const std::vector<bool>& bool_v1         = obj_l.get_bool_vec();
    const std::vector<int>& int_v1           = obj_l.get_int_vec();
    const std::vector<unsigned int>& uint_v1 = obj_l.get_uint_vec();
    const std::vector<double>& dbl_v1        = obj_l.get_dbl_vec();
   
    const unsigned int size_str1  = matr_idx1[0].size();
    const unsigned int size_chr1  = matr_idx1[1].size();
    const unsigned int size_bool1 = matr_idx1[2].size();
    const unsigned int size_int1  = matr_idx1[3].size();
    const unsigned int size_uint1 = matr_idx1[4].size();
    const unsigned int size_dbl1  = matr_idx1[5].size();

    const unsigned int size_str2  = matr_idx2[0].size();
    const unsigned int size_chr2  = matr_idx2[1].size();
    const unsigned int size_bool2 = matr_idx2[2].size();
    const unsigned int size_int2  = matr_idx2[3].size();
    const unsigned int size_uint2 = matr_idx2[4].size();
    const unsigned int size_dbl2  = matr_idx2[5].size();

    const std::vector<char>& vec_type1 = obj_l.get_typecol();
    const std::vector<char>& vec_type2 = obj_r.get_typecol();

    const std::vector<std::vector<std::string>>& tmp_val_refv1 = obj_l.get_tmp_val_refv();
    const std::vector<std::vector<std::string>>& tmp_val_refv2 = obj_r.get_tmp_val_refv();

    const std::vector<std::string>& col1 = tmp_val_refv1[key1]; 
    const std::vector<std::string>& col2 = tmp_val_refv2[key2];

    std::vector<std::vector<unsigned int>> matr_idx2b = matr_idx2;
    for (auto& el : matr_idx2b) {
      for (auto& el2 : el) {
        el2 += ncol1;
      };
    };

    matr_idx[0].insert(matr_idx[0].end(), 
                       matr_idx1[0].begin(), 
                       matr_idx1[0].end());
    matr_idx[1].insert(matr_idx[1].end(), 
                       matr_idx1[1].begin(), 
                       matr_idx1[1].end());
    matr_idx[2].insert(matr_idx[2].end(), 
                       matr_idx1[2].begin(), 
                       matr_idx1[2].end());
    matr_idx[3].insert(matr_idx[3].end(), 
                       matr_idx1[3].begin(), 
                       matr_idx1[3].end());
    matr_idx[4].insert(matr_idx[4].end(), 
                       matr_idx1[4].begin(), 
                       matr_idx1[4].end());
    matr_idx[5].insert(matr_idx[5].end(), 
                       matr_idx1[5].begin(), 
                       matr_idx1[5].end());

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

    using map_t = std::conditional_t<
        SimdHash,
        ankerl::unordered_dense::map<std::string_view, std::vector<size_t>, simd_hash>,
        ankerl::unordered_dense::map<std::string_view, std::vector<size_t>>
    >;

    map_t lookup;
    lookup.reserve(col2.size());
    for (size_t i = 0; i < col2.size(); i += 1) {
      auto [it, inserted] = lookup.try_emplace(col2[i]);
      if (inserted) {
        it->second.reserve(2);
      }
      it->second.push_back(i);
    };

    nrow = 0;

    std::vector<std::vector<size_t>> match_idx(nrow1);
    for (size_t i = 0; i < nrow1; ++i) {
        auto it = lookup.find(col1[i]);
        if (it != lookup.end()) {
            match_idx[i] = it->second;
            nrow += it->second.size();
        } else {
            nrow += 1;
        }
    }

    std::vector<size_t> rep_v(nrow1);
    for (size_t i = 0; i < nrow1; ++i) {
        const auto& matches = match_idx[i];
        rep_v[i] = matches.empty() ? 1 : matches.size();
    }

    str_v. resize(nrow * (size_str1  + size_str2),  default_str);
    chr_v. resize(nrow * (size_chr1  + size_chr2),  default_chr);
    bool_v.resize(nrow * (size_bool1 + size_bool2),  default_bool);
    int_v. resize(nrow * (size_int1  + size_int2),  default_int);
    uint_v.resize(nrow * (size_uint1 + size_uint2),  default_uint);
    dbl_v. resize(nrow * (size_dbl1  + size_dbl2),  default_dbl);

    std::vector<std::string> vec_str;
    vec_str.resize(nrow, default_str);
    tmp_val_refv.insert(tmp_val_refv.end(), ncol1 + ncol2, vec_str);

    for (size_t t = 0; t < matr_idx1[0].size(); ++t) {
        size_t dst_col = matr_idx1[0][t];

        std::vector<std::string>& val_tmp  = tmp_val_refv[dst_col];
        const std::vector<std::string>& val_tmp2 = tmp_val_refv1[dst_col];

        auto*       dst_val = str_v.data()  + nrow  * t;
        const auto* src_val = str_v1.data() + nrow1 * t;
        
        size_t out = 0;
        for (size_t i_ref = 0; i_ref < nrow1; ++i_ref) {
            const size_t repeat = rep_v[i_ref];
        
            const std::string& v1 = src_val[i_ref];
            const std::string& v2 = val_tmp2[i_ref];
        
            for (size_t r = 0; r < repeat; ++r, ++out) {
                dst_val[out] = v1;
                val_tmp[out] = v2;
            }
        }
    }

    for (size_t t = 0; t < matr_idx2b[0].size(); ++t) {
        size_t dst_col = matr_idx2b[0][t];
        size_t src_col = matr_idx2[0][t];

        std::vector<std::string>& val_tmp  = tmp_val_refv[dst_col];
        const std::vector<std::string>& val_tmp2 = tmp_val_refv2[src_col];

        auto*       dst_val = str_v.data()  + nrow  * (size_str1 + t);
        const auto* src_val = str_v2.data() + nrow2 * t;

        size_t out = 0;  
        for (size_t i_ref = 0; i_ref < nrow1; ++i_ref) {
            const auto& matches = match_idx[i_ref]; 

            if (!matches.empty()) {
                for (size_t j_idx : matches) {
                    dst_val[out] = src_val[j_idx];
                    val_tmp[out] = val_tmp2[j_idx];
                    ++out;
                }
            } else {
                ++out;
            }
        }

    }


    for (size_t t = 0; t < matr_idx1[1].size(); ++t) {
        size_t dst_col = matr_idx1[1][t];

        std::vector<std::string>& val_tmp  = tmp_val_refv[dst_col];
        const std::vector<std::string>& val_tmp2 = tmp_val_refv1[dst_col];

        auto*       dst_val = chr_v.data()  + nrow  * t;
        const auto* src_val = chr_v1.data() + nrow1 * t;
        
        size_t out = 0;
        for (size_t i_ref = 0; i_ref < nrow1; ++i_ref) {
            const size_t repeat = rep_v[i_ref];
        
            const char& v1 = src_val[i_ref];
            const std::string& v2 = val_tmp2[i_ref];
        
            for (size_t r = 0; r < repeat; ++r, ++out) {
                dst_val[out] = v1;
                val_tmp[out] = v2;
            }
        }
    }

    for (size_t t = 0; t < matr_idx2b[1].size(); ++t) {
        size_t dst_col = matr_idx2b[1][t];
        size_t src_col = matr_idx2[1][t];

        std::vector<std::string>& val_tmp  = tmp_val_refv[dst_col];
        const std::vector<std::string>& val_tmp2 = tmp_val_refv2[src_col];

        auto*       dst_val = chr_v.data()  + nrow  * (size_chr1 + t);
        const auto* src_val = chr_v2.data() + nrow2 * t;

        size_t out = 0;  
        for (size_t i_ref = 0; i_ref < nrow1; ++i_ref) {
            const auto& matches = match_idx[i_ref]; 

            if (!matches.empty()) {
                for (size_t j_idx : matches) {
                    dst_val[out] = src_val[j_idx];
                    val_tmp[out] = val_tmp2[j_idx];
                    ++out;
                }
            } else {
                ++out;
            }
        }

    }


    for (size_t t = 0; t < matr_idx1[2].size(); ++t) {
        size_t dst_col = matr_idx1[2][t];

        std::vector<std::string>& val_tmp  = tmp_val_refv[dst_col];
        const std::vector<std::string>& val_tmp2 = tmp_val_refv1[dst_col];

        auto       dst_val = bool_v.begin()  + nrow  * t;
        const auto src_val = bool_v1.begin() + nrow1 * t;
        
        size_t out = 0;
        for (size_t i_ref = 0; i_ref < nrow1; ++i_ref) {
            const size_t repeat = rep_v[i_ref];

        
            const bool& v1 = *(src_val + i_ref);
            const std::string& v2 = val_tmp2[i_ref];
        
            for (size_t r = 0; r < repeat; ++r, ++out) {
                dst_val[out] = v1;
                val_tmp[out] = v2;
            }
        }
    }

    for (size_t t = 0; t < matr_idx2b[2].size(); ++t) {
        size_t dst_col = matr_idx2b[2][t];
        size_t src_col = matr_idx2[2][t];

        std::vector<std::string>& val_tmp  = tmp_val_refv[dst_col];
        const std::vector<std::string>& val_tmp2 = tmp_val_refv2[src_col];

        auto       dst_val = bool_v.begin()  + nrow  * (size_bool1 + t);
        const auto src_val = bool_v2.begin() + nrow2 * t;

        size_t out = 0;  
        for (size_t i_ref = 0; i_ref < nrow1; ++i_ref) {
            const auto& matches = match_idx[i_ref]; 

            if (!matches.empty()) {
                for (size_t j_idx : matches) {
                    dst_val[out] = src_val[j_idx];
                    val_tmp[out] = val_tmp2[j_idx];
                    ++out;
                }
            } else {
                ++out;
            }
        }

    }


    for (size_t t = 0; t < matr_idx1[3].size(); ++t) {
        size_t dst_col = matr_idx1[3][t];

        std::vector<std::string>& val_tmp  = tmp_val_refv[dst_col];
        const std::vector<std::string>& val_tmp2 = tmp_val_refv1[dst_col];

        auto*       dst_val = int_v.data()  + nrow  * t;
        const auto* src_val = int_v1.data() + nrow1 * t;
        
        size_t out = 0;
        for (size_t i_ref = 0; i_ref < nrow1; ++i_ref) {
            const size_t repeat = rep_v[i_ref];
        
            const IntT& v1 = src_val[i_ref];
            const std::string& v2 = val_tmp2[i_ref];
        
            for (size_t r = 0; r < repeat; ++r, ++out) {
                dst_val[out] = v1;
                val_tmp[out] = v2;
            }
        }
    }

    for (size_t t = 0; t < matr_idx2b[3].size(); ++t) {
        size_t dst_col = matr_idx2b[3][t];
        size_t src_col = matr_idx2[3][t];

        std::vector<std::string>& val_tmp  = tmp_val_refv[dst_col];
        const std::vector<std::string>& val_tmp2 = tmp_val_refv2[src_col];

        auto*       dst_val = int_v.data()  + nrow  * (size_int1 + t);
        const auto* src_val = int_v2.data() + nrow2 * t;

        size_t out = 0;  
        for (size_t i_ref = 0; i_ref < nrow1; ++i_ref) {
            const auto& matches = match_idx[i_ref]; 

            if (!matches.empty()) {
                for (size_t j_idx : matches) {
                    dst_val[out] = src_val[j_idx];
                    val_tmp[out] = val_tmp2[j_idx];
                    ++out;
                }
            } else {
                ++out;
            }
        }

    }


    for (size_t t = 0; t < matr_idx1[4].size(); ++t) {
        size_t dst_col = matr_idx1[4][t];

        std::vector<std::string>& val_tmp  = tmp_val_refv[dst_col];
        const std::vector<std::string>& val_tmp2 = tmp_val_refv1[dst_col];

        auto*       dst_val = uint_v.data()  + nrow  * t;
        const auto* src_val = uint_v1.data() + nrow1 * t;
        
        size_t out = 0;
        for (size_t i_ref = 0; i_ref < nrow1; ++i_ref) {
            const size_t repeat = rep_v[i_ref];
        
            const UIntT& v1 = src_val[i_ref];
            const std::string& v2 = val_tmp2[i_ref];
        
            for (size_t r = 0; r < repeat; ++r, ++out) {
                dst_val[out] = v1;
                val_tmp[out] = v2;
            }
        }
    }

    for (size_t t = 0; t < matr_idx2b[4].size(); ++t) {
        size_t dst_col = matr_idx2b[4][t];
        size_t src_col = matr_idx2[4][t];

        std::vector<std::string>& val_tmp  = tmp_val_refv[dst_col];
        const std::vector<std::string>& val_tmp2 = tmp_val_refv2[src_col];

        auto*       dst_val = uint_v.data()  + nrow  * (size_uint1 + t);
        const auto* src_val = uint_v2.data() + nrow2 * t;

        size_t out = 0;  
        for (size_t i_ref = 0; i_ref < nrow1; ++i_ref) {
            const auto& matches = match_idx[i_ref]; 

            if (!matches.empty()) {
                for (size_t j_idx : matches) {
                    dst_val[out] = src_val[j_idx];
                    val_tmp[out] = val_tmp2[j_idx];
                    ++out;
                }
            } else {
                ++out;
            }
        }

    }


    for (size_t t = 0; t < matr_idx1[5].size(); ++t) {
        size_t dst_col = matr_idx1[5][t];

        std::vector<std::string>& val_tmp  = tmp_val_refv[dst_col];
        const std::vector<std::string>& val_tmp2 = tmp_val_refv1[dst_col];

        auto*       dst_val = dbl_v.data()  + nrow  * t;
        const auto* src_val = dbl_v1.data() + nrow1 * t;
        
        size_t out = 0;
        for (size_t i_ref = 0; i_ref < nrow1; ++i_ref) {
            const size_t repeat = rep_v[i_ref];
        
            const FloatT& v1 = src_val[i_ref];
            const std::string& v2 = val_tmp2[i_ref];
        
            for (size_t r = 0; r < repeat; ++r, ++out) {
                dst_val[out] = v1;
                val_tmp[out] = v2;
            }
        }
    }

    for (size_t t = 0; t < matr_idx2b[5].size(); ++t) {
        size_t dst_col = matr_idx2b[5][t];
        size_t src_col = matr_idx2[5][t];

        std::vector<std::string>& val_tmp  = tmp_val_refv[dst_col];
        const std::vector<std::string>& val_tmp2 = tmp_val_refv2[src_col];

        auto*       dst_val = dbl_v.data()  + nrow  * (size_dbl1 + t);
        const auto* src_val = dbl_v2.data() + nrow2 * t;

        size_t out = 0;  
        for (size_t i_ref = 0; i_ref < nrow1; ++i_ref) {
            const auto& matches = match_idx[i_ref]; 

            if (!matches.empty()) {
                for (size_t j_idx : matches) {
                    dst_val[out] = src_val[j_idx];
                    val_tmp[out] = val_tmp2[j_idx];
                    ++out;
                }
            } else {
                ++out;
            }
        }

    }

    type_refv.insert(type_refv.end(), vec_type1.begin(), vec_type1.end());
    type_refv.insert(type_refv.end(), vec_type2.begin(), vec_type2.end());

    ncol += (ncol1 + ncol2);

    const std::vector<std::string>& colname1 = obj_l.get_colname();
    const std::vector<std::string>& colname2 = obj_r.get_colname();

    if (colname2.size() > 0 && colname1.size() > 0) {
      name_v.insert(name_v.end(), colname1.begin(), colname1.end());
      name_v.insert(name_v.end(), colname2.begin(), colname2.end());
    } else {
      name_v.resize(ncol);
    };

};




