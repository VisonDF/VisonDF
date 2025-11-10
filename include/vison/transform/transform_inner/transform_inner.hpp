#pragma once

template <bool MemClean = false, bool SimdHash = true>
void transform_inner(Dataframe &cur_obj, 
                unsigned int &in_col, 
                unsigned int &ext_col) 
{
  
    const auto& cur_tmp = cur_obj.get_tmp_val_refv();
    const std::vector<std::string>& ext_colv = cur_tmp[ext_col];
    const std::vector<std::string>& in_colv = tmp_val_refv[in_col];
    const unsigned int& ext_nrow = cur_obj.get_nrow();
    const unsigned int nrow2 = nrow;

    //std::unordered_set<std::string_view> lookup // standard set (slower);

    using fast_str_set_t = std::conditional_t<
        SimdHash,
        ankerl::unordered_dense::set<std::string_view, simd_hash>,
        ankerl::unordered_dense::set<std::string_view>
    >;

    fast_str_set_t lookup;
    lookup.reserve(ext_nrow);

    for (const auto& el : ext_colv)
      lookup.insert(el);

    std::vector<uint8_t> mask(nrow2);
    for (unsigned int i = 0; i < nrow2; ++i)
        mask[i] = lookup.contains(in_colv[i]);

    this->transform_filter<MemClean>(mask);
    
};


