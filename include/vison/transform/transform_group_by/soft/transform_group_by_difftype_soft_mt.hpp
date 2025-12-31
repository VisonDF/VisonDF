#pragma once

template <typename TContainer = void,
          unsigned int CORES = 4,
          bool SimdHash = true,
          bool MapCol = false,
          unsigned int NPerGroup = 4>
void transform_group_by_difftype_soft_mt(const std::vector<unsigned int>& x) 
{

    unsigned int I = 0;
    for (auto& el : grp_by_col) {
        if (contains_all(el, x)) {
            transform_group_by_soft_alrd_mt<CORES, 
                                            NPerGroup>(I, n);
            return;
        }
        I += 1;
    }

    const bool RsltTypeKnown = true;

    using key_t = std::string_view;

    using map_t = std::conditional_t<
        SimdHash,
        ankerl::unordered_dense::map<key_t, ReservingVec<unsigned int>, simd_hash>,
        ankerl::unordered_dense::map<key_t, ReservingVec<unsigned int>>
    >;
    map_t var_lookup;

    std::vector<unsigned int> idx_str;
    std::vector<unsigned int> idx_chr;
    std::vector<unsigned int> idx_bool;
    std::vector<unsigned int> idx_int;
    std::vector<unsigned int> idx_uint;
    std::vector<unsigned int> idx_dbl;

    const unsigned int local_nrow = nrow;
    constexpr int key_idx;

    vecs_build_difftype(x,
                        idx_str,
                        idx_chr,
                        idx_bool,
                        idx_int,
                        idx_uint,
                        idx_dbl,
                        local_nrow);

    constexpr size_t val_size;
    std::vector<std::string_key*> key_vec; // dummy key_vec
    constexpr int val_col; // dummy val_col
    constexpr int key_col; // dummy val_col

    ReservingVec<unsigned int> vec(NPerGroup);

    if constexpr (CORES == 1) {

        SameDiffTypeSoft::template apply<TContainer, 
                                         KeyBuildDiffType>(val_col,
                                                           0,
                                                           local_nrow,
                                                           lookup,
                                                           vec,
                                                           key_col,
                                                           val_size,
                                                           key_idx,
                                                           idx_str,
                                                           idx_chr,
                                                           idx_bool,
                                                           idx_int,
                                                           idx_uint,
                                                           idx_dbl,
                                                           key_vec
                                                           );


    } else if constexpr (CORES > 1) {

        const unsigned int chunks = local_nrow / CORES + 1;
        std::vector<map_t> vec_map(CORES);

        #pragma omp parallel num_threads(CORES)
        {
            const unsigned int tid   = omp_get_thread_num();
            const unsigned int start = tid * chunks;
            const unsigned int end   = std::min(local_nrow, start + chunks);
            map_t& cur_map           = vec_map[tid];

            SameDiffTypeSoft::template apply<TContainer, 
                                             KeyBuildDiffType>(val_col,
                                                               start,
                                                               end,
                                                               lookup,
                                                               vec,
                                                               key_col,
                                                               val_size,
                                                               key_idx,
                                                               idx_str,
                                                               idx_chr,
                                                               idx_bool,
                                                               idx_int,
                                                               idx_uint,
                                                               idx_dbl,
                                                               key_vec
                                                               );
        }

        merge_soft<NPerGroup>(vec_map, lookup);

    }

    if (!in_view) {
        in_view = true;
        row_view_idx.resize(local_nrow);
        row_view_map.reserve(local_nrow);
        for (size_t i = 0; i < local_nrow; ++i)
            row_view_map.emplace(i, i);
    }

    create_value_col_soft<CORES>(lookup, local_nrow);

    for (size_t i = 0; i < local_nrow; ++i)
        row_view_map[i] = row_view_idx[i];
    
}



