#pragma once

template <bool SimdHash = true>
void transform_group_by(std::vector<unsigned int>& x,
                        std::string sumcolname = "n") 
{
    using map_t = std::conditional_t<
        SimdHash,
        ankerl::unordered_dense::map<std::string, unsigned int, simd_hash>,
        ankerl::unordered_dense::map<std::string, unsigned int>
    >;

    map_t lookup;
    lookup.reserve(nrow);

    std::vector<std::string_view> key_vec(nrow);

    std::string key;
    const size_t total_key_len = 128;
    key.reserve(total_key_len); 

    for (unsigned int i = 0; i < nrow; ++i) {

        key.clear();
        if (key.capacity() < total_key_len) {
            key.reserve(total_key_len); 
        }
        char* dst = key.data(); 
        for (size_t j = 0; j < x.size(); ++j) { 
            const auto& src = tmp_val_refv[x[j]][i]; 
            memcpy(dst, src.data(), src.size()); 
            dst += src.size(); 
            *dst++ = '\x1F'; 
        }

        const size_t used = dst - key.data();
        key.resize(used);  

        auto [it, inserted] = lookup.try_emplace(std::move(key), 0);
        ++(it->second);
        key_vec[i] = it->first;
    }

    std::vector<unsigned int> occ_v(nrow);
    std::vector<std::string> occ_v_str(nrow);

    size_t i = 0;
    for (auto& key : key_vec) {
        unsigned int count = lookup[key];
        occ_v[i] = count;
    
        char buf[max_chars_needed<unsigned int>()];
        auto [ptr, ec] = std::to_chars(buf, buf + sizeof(buf), count);
        occ_v_str[i].assign(buf, ptr);
    
        ++i;
    }
    
    uint_v.insert(uint_v.end(), occ_v.begin(), occ_v.end());
    tmp_val_refv.push_back(std::move(occ_v_str));

    if (!name_v.empty())
        name_v.push_back(sumcolname);

    type_refv.push_back('u');
    ++ncol;
}


