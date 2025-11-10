#pragma once

template <bool SimdHash = true, bool MemClean = false>
void transform_unique(unsigned int& n) {
  
    //std::unordered_set<std::string> unic_v; // standard set (slower)
   
    const unsigned int nrow2 = nrow;

    using fast_str_set_t = std::conditional_t<
        SimdHash,
        ankerl::unordered_dense::set<std::string_view, simd_hash>,
        ankerl::unordered_dense::set<std::string_view>
    >;

    fast_str_set_t unic_v;
    unic_v.reserve(nrow2);

    std::vector<uint8_t> mask(nrow2, 0);
    const std::vector<std::string>& val_tmp = tmp_val_refv[n];

    for (unsigned int i = 0; i < nrow2; ++i) {
        std::string_view view_val = val_tmp[i];
        if (!unic_v.contains(view_val)) {
            unic_v.insert(view_val);
            mask[i] = 1;
        }
    }

    this->transform_filter(mask);

};





