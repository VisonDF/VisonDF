#pragma once

template <typename TContainer = void,
          unsigned int CORES = 4,
          bool SimdHash = true,
          bool MapCol = false,
          unsigned int NPerGroup>
void transform_group_by_onecol_soft_mt(unsigned int x) 
{

    unsigned int I = 0;
    for (auto& el : grp_by_col) {
        if (el.size() == 1) {
            if (el.contains(x)) {
                transform_group_by_soft_alrd_mt<CORES, 
                                                NPerGroup>(I, n);
                return;
            }
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

    const unsigned int local_nrow = nrow;

    using key_variant_t = std::conditional_t<
                              !std::is_same_v<TContainer, void>,
                              const std::vector<std::vector<element_type_t<TContainer>>>*,
                              std::variant<
                                  std::monostate,
                                  const std::vector<std::vector<std::string>>*,
                                  const std::vector<std::vector<CharT>>*,
                                  const std::vector<std::vector<uint8_t>>*,
                                  const std::vector<std::vector<IntT>>*,
                                  const std::vector<std::vector<UIntT>>*,
                                  const std::vector<std::vector<FloatT>>*
                              >
    >; 

    key_variant_t var_key_table;
    unsigned int idx_type      = key_table_build<TContainer>(var_key_table, x);
    const unsigned int key_idx = idx_build_onecol<MapCol>(x, idx_type);

    const std::vector<std::vector<uint8_t>>* var_val_table = &bool_v; // dummy var_val_table

    map_t var_lookup;

    constexpr auto& size_table = get_types_size();
    const size_t val_size      = size_table[idx_type];

    std::vector<std::string_key*> key_vec; // dummy key_vec

    if constexpr (CORES == 1) {

        // KeyBuildSameType is just for completing a dummy placeholder
        dispatch1_onecol<GroupFunction::Occurence, // dummy function 
                         3, // soft mode
                         element_type_t<TContainer>,
                         uint8_t, // dumy type (TColVal)
                         OneColSoft,
                         KeyBuildSameType>(0, 
                                           local_nrow, 
                                           var_lookup,
                                           val_size,
                                           key_idx,
                                           val_idx,
                                           var_key_table,
                                           var_val_table,
                                           NPerGroup,
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

            // KeyBuildSameType is just for completing a dummy placeholder
            dispatch1_onecol<GroupFunction::Occurence, // dummy function
                             3, // soft mode
                             element_type_t<TContainer>,
                             uint8_t, // dummy type (TColVal)
                             OneColSoft,
                             KeyBuildSameType>(start, 
                                               end, 
                                               cur_map,
                                               val_size,
                                               key_idx,
                                               val_idx,
                                               var_key_table,
                                               var_val_table,
                                               NPerGroup,
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




