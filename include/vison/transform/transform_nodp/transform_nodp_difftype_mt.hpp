#pragma once

template <unsigned int CORES = 4,
          bool MemClean      = false, 
          bool Last          = false,
          bool MapCol        = false,
          bool Soft          = true,
          bool SimdHash      = true>
void transform_nodp_difftype_mt(std::vector<unsigned int>& n) 
{  

    if (in_view && !Soft) {
        std::cerr << "Can't perform this operation while in `view` mode, consider applying `.materialize()`\n"
        return;
    }

    const size_t local_nrow = nrow;
    std::vector<uint8_t> mask(local_nrow, 1);

    using fast_set_t = std::conditional_t<
        SimdHash,
        ankerl::unordered_dense::set<std::string_view, simd_hash>,
        ankerl::unordered_dense::set<std::string_view>
    >;

    fast_set_t lookup;
    lookup.reserve(local_nrow);

    unsigned int real_pos;

    if constexpr (!MapCol) {

        std::unordered_map<unsigned int, unsigned int> pos;
        for (size_t t = 0; t < matr_idx[idx_type].size(); ++t)
            pos[matr_idx[idx_type]] = t;
 
        real_pos = pos[n];

    } else {

        if (!matr_idx_map[idx_type].contains(n)) {
            std::cerr << "MapCol has been chosen, not found in the mapcol\n";
            return;
        }

        if (!sync_map_col[idx_type]) {
            std::cerr << "MapCol is not synced\n";
            return;
        }

        real_pos = matr_idx_map[idx_type][n]; 
    }

    std::vector<size_t> idx_str;
    std::vector<size_t> idx_chr;
    std::vector<size_t> idx_bool;
    std::vector<size_t> idx_int;
    std::vector<size_t> idx_uint;
    std::vector<size_t> idx_dbl;

    vecs_difftype_build<MapCol>(
                                x,
                                idx_str,
                                idx_chr,
                                idx_bool,
                                idx_int,
                                idx_uint,
                                idx_dbl,
                                ncol
                                );

    std::string key_str;
    key_str.reserve(512);

    if constexpr (!Last) {
         for (size_t i = 0; i < local_nrow; ++i) {
             key_str.clear();
             for (auto ncol : idx_str) {
                key_str += str_v[ncol][i];
                key_str += "_";
             }
             for (auto ncol : idx_chr) {
                key_str.append(reinterpret_cast<const char*>(&chr_v[n][i],  sizeof(CharT)));
                key_str += "_";
             }
             for (auto ncol : idx_bool) {
                key_str.append(reinterpret_cast<const char*>(&bool_v[n][i], sizeof(uint8_t)));
                key_str += "_";
             }
             for (auto ncol : idx_int) {
                key_str.append(reinterpret_cast<const char*>(&int_v[n][i],  sizeof(IntT)));
                key_str += "_";
             }
             for (auto ncol : idx_uint) {
                key_str.append(reinterpret_cast<const char*>(&uint_v[n][i], sizeof(UIntT)));
                key_str += "_";
             }
             for (auto ncol : idx_dbl) {
                key_str.append(reinterpret_cast<const char*>(&dbl_v[n][i],  sizeof(FloatT)));
                key_str += "_";
             }
             if (!lookup.contains(key_str)) {
                 mask[i] = 0;
                 lookup.emplace(key_str);
             }
         }
    } else {
        for (int i = int(local_nrow) - 1; i >= 0; --i) {
             key_str.clear();
             for (auto ncol : idx_str) {
                key_str += str_v[ncol][i];
                key_str += "_";
             }
             for (auto ncol : idx_chr) {
                key_str.append(reinterpret_cast<const char*>(&chr_v[n][i],  sizeof(CharT)));
                key_str += "_";
             }
             for (auto ncol : idx_bool) {
                key_str.append(reinterpret_cast<const char*>(&bool_v[n][i], sizeof(uint8_t)));
                key_str += "_";
             }
             for (auto ncol : idx_int) {
                key_str.append(reinterpret_cast<const char*>(&int_v[n][i],  sizeof(IntT)));
                key_str += "_";
             }
             for (auto ncol : idx_uint) {
                key_str.append(reinterpret_cast<const char*>(&uint_v[n][i], sizeof(UIntT)));
                key_str += "_";
             }
             for (auto ncol : idx_dbl) {
                key_str.append(reinterpret_cast<const char*>(&dbl_v[n][i],  sizeof(FloatT)));
                key_str += "_";
             }
             if (!lookup.contains(key_str)) {
                 mask[i] = 0;
                 lookup.emplace(key_str);
             }
        }
    }

    this->transform_filter_mt<CORES,
                              MemClean, 
                              false, //SmallProportion
                              Soft>(mask);

};










