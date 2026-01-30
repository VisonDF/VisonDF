#pragma once

template <unsigned int CORES = 4, 
          bool NUMA = false,
          bool Nested = true, 
          bool SimdHash = true>
void otm_mt(Dataframe &obj_l,
            Dataframe &obj_r,
            const unsigned int &key1, 
            const unsigned int &key2,
            const std::string default_str = "NA",
            const uint8_t default_bool = 0,
            const IntT default_int = 0,
            const UIntT default_uint = 0,
            const FloatT default_dbl = 0) 
{
  
    const unsigned int& ncol1 = obj_l.get_ncol();
    const unsigned int& ncol2 = obj_r.get_ncol();

    const unsigned int& nrow1 = obj_l.get_nrow();
    const unsigned int& nrow2 = obj_r.get_nrow();

    const std::vector<std::vector<unsigned int>>& matr_idx1 = obj_l.get_matr_idx();
    const std::vector<std::vector<unsigned int>>& matr_idx2 = obj_r.get_matr_idx();

    const auto& str_v2  = obj_r.get_str_vec();
    const auto& chr_v2  = obj_r.get_chr_vec();
    const auto& bool_v2 = obj_r.get_bool_vec();
    const auto& int_v2  = obj_r.get_int_vec();
    const auto& uint_v2 = obj_r.get_uint_vec();
    const auto& dbl_v2  = obj_r.get_dbl_vec();
 
    const auto& str_v1  = obj_l.get_str_vec();
    const auto& chr_v1  = obj_l.get_chr_vec();
    const auto& bool_v1 = obj_l.get_bool_vec();
    const auto& int_v1  = obj_l.get_int_vec();
    const auto& uint_v1 = obj_l.get_uint_vec();
    const auto& dbl_v1  = obj_l.get_dbl_vec();
   
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

    using var_col = std

    for (size_t i = 0; i < col2.size(); i += 1) {
      auto [it, inserted] = lookup.try_emplace(col2[i], {});
      if (inserted) {
        it->second.reserve(2);
      }
      it->second.push_back(i);
    };

    const int prev_nested = omp_get_nested();
    const int prev_max_levels = omp_get_max_active_levels();

    if constexpr (Nested)
        omp_set_nested(1);
    else
        omp_set_nested(0);

    unsigned int outer_threads = std::min<unsigned int>(
        matr_idx1[0].size() + matr_idx2b[0].size(),
        std::max(1u, CORES / 2)
    );
    unsigned int inner_threads = std::max(1u, CORES / outer_threads);

    omp_set_max_active_levels(Nested ? 2 : 1);

    std::vector<std::vector<size_t>> match_idx(nrow1, {});
    std::vector<size_t> rep_v(nrow1, 0);
    
    #pragma omp parallel for num_threads(outer_threads) schedule(static)
    for (ptrdiff_t i = 0; i < static_cast<ptrdiff_t>(nrow1); ++i) {
        auto it = lookup.find(col1[i]);
        if (it != lookup.end()) {
            match_idx[i] = it->second;          
            rep_v[i]     = match_idx[i].size(); 
        } else {
            rep_v[i] = 1;
        }
    }

    // calculate the tot number of rows safely
    std::vector<size_t> out_offset;
    if constexpr (Nested) {
        out_offset.resize(nrow1, 0);
        for (size_t i = 1; i < nrow1; ++i)
           out_offset[i] = out_offset[i - 1] + rep_v[i - 1];
        nrow = out_offset.back() + rep_v.back();
    } else {
        size_t tot_nb = 0;
        for (size_t i = 0; i < nrow1; ++i)
           tot_nb += rep_v[i];
        nrow = tot_nb;
    }
    const unsigned int local_nrow_final = nrow;

    str_v. resize(size_str1  + size_str2);
    chr_v. resize(size_chr1  + size_chr2);
    bool_v.resize(size_bool1 + size_bool2);
    int_v. resize(size_int1  + size_int2);
    uint_v.resize(size_uint1 + size_uint2);
    dbl_v. resize(size_dbl1  + size_dbl2);

    for (auto& el : str_v)
        el.resize(local_nrow_final);
    for (auto& el : chr_v)
        el.resize(local_nrow_final);
    for (auto& el : bool_v)
        el.resize(local_nrow_final);
    for (auto& el : int_v)
        el.resize(local_nrow_final);
    for (auto& el : uint_v)
        el.resize(local_nrow_final);
    for (auto& el : dbl_v)
        el.resize(local_nrow_final);

    auto expand_repeats = [&rep_v,
                           &out_offset]<typename T>(
                                                     std::vector<std::vector<T>>& dst_vec,
                                                     std::vector<std::vector<T>>& src_vec,
                                                     const std::vector<size_t>& idx_list,
                                                   )
    {
        #pragma omp parallel for num_threads(outer_threads) schedule(static)
        for (ptrdiff_t t = 0; t < static_cast<ptrdiff_t>(idx_list.size()); ++t) {
    
            T*       dst_val = dst_vec[static_cast<size_t>(t)].data();
            const T* src_val = src_vec[static_cast<size_t>(t)].data();
    
            if constexpr (Nested) {
    
                #pragma omp parallel for num_threads(inner_threads) schedule(static)
                for (size_t i_ref = 0; i_ref < nrow1; ++i_ref) {
                    const size_t repeat = rep_v[i_ref];
    
                    const T& v1              = src_val[i_ref];
                    const size_t out         = out_offset[i_ref];

                    if constexpr (sizeof(T) == 1) {
                        memset(dst_val + out, v1, repeat;
                    } else {
                        std::fill_n((*dst_val).begin(), repeat, v1);
                    }

                }
    
            } else {
    
                size_t out = 0;
    
                for (size_t i_ref = 0; i_ref < nrow1; ++i_ref) {
                    const size_t repeat     = rep_v[i_ref];
    
                    const T& v1             = src_val[i_ref];

                    if constexpr (sizeof(T) == 1) {
                        memset(dst_val + out, v1, repeat);
                    } else {
                        std::fill_n((*dst_val).begin(), repeat, v1);
                    }

                    out += repeat;
    
                }
            }
        }
    };

    auto expand_matches = [&out_offset]<typename T>(std::vector<std::vector<T>>& dst_vec,
                                                    std::vector<std::vector<T>>& src_vec,
                                                    const std::vector<size_t>& idx_list_b,
                                                    const std::vector<size_t>& idx_list,
                                                    const size_t offset)
    {
        using T = typename std::remove_reference_t<decltype(dst_vec)>::value_type;
    
        #pragma omp parallel for num_threads(outer_threads) schedule(static)
        for (ptrdiff_t t = 0; t < static_cast<ptrdiff_t>(idx_list_b.size()); ++t) {
    
            T&       dst_val = dst_vec[static_cast<size_t>(t) + offset];
            const T& src_val = src_vec[static_cast<size_t>(t)];
    
            if constexpr (Nested) {
    
                #pragma omp parallel for num_threads(inner_threads) schedule(static)
                for (ptrdiff_t i_ref = 0; i_ref < static_cast<ptrdiff_t>(nrow1); ++i_ref) {
    
                    size_t out        = out_offset[static_cast<size_t>(i_ref)];
                    auto& matches     = match_idx[static_cast<size_t>(i_ref)];
    
                    if (matches.empty())
                        continue;
    
                     for (size_t j_idx : matches) 
                         dst_val[out++] = src_val[j_idx];
    
                }
    
            } else {
    
                size_t out = 0;
    
                for (size_t i_ref = 0; i_ref < nrow1; ++i_ref) {
    
                    auto& matches = match_idx[i_ref];
    
                    if (matches.empty()) {
                        ++out;
                        continue;
                    }
    
                    for (size_t j_idx : matches)
                        dst_val[out++] = src_val[j_idx];
    
                }
            }
        }
    };

    expand_repeats(str_v, 
                   str_v1, 
                   matr_idx1[0]);
    expand_matches(str_v, 
                   str_v2,
                   matr_idx2b[0], 
                   matr_idx2[0],
                   size_str1);

    expand_repeats(chr_v, 
                   chr_v1, 
                   matr_idx1[1]);
    expand_matches(chr_v, 
                   chr_v2,
                   matr_idx2b[1], 
                   matr_idx2[1],
                   size_chr1);

    expand_repeats(bool_v, 
                   bool_v1, 
                   matr_idx1[2]);
    expand_matches(bool_v, 
                   bool_v2,
                   matr_idx2b[2], 
                   matr_idx2[2],
                   size_bool1);

    expand_repeats(int_v, 
                   int_v1, 
                   matr_idx1[3]);
    expand_matches(int_v, 
                   int_v2,
                   matr_idx2b[3], 
                   matr_idx2[3],
                   size_int1);

    expand_repeats(uint_v, 
                   uint_v1, 
                   matr_idx1[4]);
    expand_matches(uint_v, 
                   uint_v2,
                   matr_idx2b[4], 
                   matr_idx2[4],
                   size_uint1);

    expand_repeats(dbl_v, 
                   dbl_v1, 
                   matr_idx1[5]);
    expand_matches(dbl_v, 
                   dbl_v2,
                   matr_idx2b[5], 
                   matr_idx2[5],
                   size_dbl1);


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

    omp_set_nested(prev_nested);
    omp_set_max_active_levels(prev_max_levels);

};




