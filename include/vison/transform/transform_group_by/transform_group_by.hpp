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

    std::vector<unsigned int> occ_v;
    occ_v.reserve(nrow);
    std::vector<std::string> occ_v_str;
    occ_v_str.reserve(nrow);

    std::vector<std::string> key_vec(nrow);

    std::string key;
    key.reserve(128); 

    for (unsigned int i = 0; i < nrow; ++i) {
        key.clear();
        for (size_t j = 0; j < x.size(); ++j) {
            key += tmp_val_refv[x[j]][i];
            key += '\x1F';
        }

        auto [it, inserted] = lookup.try_emplace(key, 0);
        ++(it->second);
        key_vec[i] = it->first;
    }

    occ_v.resize(nrow);
    occ_v_str.resize(nrow);
    
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


