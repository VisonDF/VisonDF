#pragma once

template <typename TContainer = void,
          typename TColVal = void,
          unsigned int CORES = 4,
          GroupFunction Function = GroupFunction::Ocurence,
          bool SimdHash = true,
          bool MapCol = false,
          bool StandardMethod = true,
          unsigned int NPerGroup = 4,
          typename F = decltype(&default_groupfn_impl)>
requires GroupFn<F, first_arg_grp_t<F>>
void transform_group_by_sametype_mt(const std::vector<unsigned int>& x,
                                    const n_col int,
                                    const std::string colname = "n",
                                    F f = &default_groupfn_impl) 
{

    if constexpr (Function != GroupFunction::Occurence) {
        if (n_col > ncol) {
            std::cerr << "Column number out of range\n";
            return;
        }
    }

    unsigned int I = 0;
    for (auto& el : grp_by_col) {
        if (contains_all(el, x)) {
            transform_group_by_alrd_mt<CORES, 
                                       NPerGroup>(I, n, colname);
            return;
        }
        I += 1;
    }

    constexpr bool RsltTypeKnown = (!(std::is_same_v<TColVal, void>) && 
                                     Function != GroupFunction::Gather);

    using key_t = std::string_view;
    using col_value_t = std::conditional_t<Function == GroupFunction::Occurence, 
                                           std::vector<UIntT>,
                                           std::conditional_t<
                                               RsltTypeKnown,
                                               std::vector<element_type_t<element_type_t<TColVal>>>,
                                               std::variant<
                                                    std::monostate,
                                                    std::vector<std::string>, 
                                                    std::vector<CharT>, 
                                                    std::vector<uint8_t>, 
                                                    std::vector<IntT>, 
                                                    std::vector<UIntT>, 
                                                    std::vector<FloatT>
                                               >
                                           >
                                          >;

    using map_t = std::conditional_t<
        SimdHash,
        std::conditional_t<
            Function == GroupFunction::Occurence,
            ankerl::unordered_dense::map<key_t, PairGroupBy<UIntT>, simd_hash>,   
            std::conditional_t<
                !(std::is_same_v<TColVal, void>),
                ankerl::unordered_dense::map<key_t, PairGroupBy<element_type_t<TColVal>>, simd_hash>,
                std::variant<
                    std::monostate,
                    ankerl::unordered_dense::map<key_t, std::string,              simd_hash>, 
                    ankerl::unordered_dense::map<key_t, CharT,                    simd_hash>, 
                    ankerl::unordered_dense::map<key_t, uint8_t,                  simd_hash>, 
                    ankerl::unordered_dense::map<key_t, IntT,                     simd_hash>, 
                    ankerl::unordered_dense::map<key_t, UIntT,                    simd_hash>, 
                    ankerl::unordered_dense::map<key_t, FloatT,                   simd_hash>,
                    ankerl::unordered_dense::map<key_t, ReservingVec<std::string>, simd_hash>, 
                    ankerl::unordered_dense::map<key_t, ReservingVec<CharT>,       simd_hash>, 
                    ankerl::unordered_dense::map<key_t, ReservingVec<uint8_t>,     simd_hash>, 
                    ankerl::unordered_dense::map<key_t, ReservingVec<IntT>,        simd_hash>, 
                    ankerl::unordered_dense::map<key_t, ReservingVec<UIntT>,       simd_hash>, 
                    ankerl::unordered_dense::map<key_t, ReservingVec<FloatT>,      simd_hash>
                >
            >
        >,
        std::conditional_t<
            Function == GroupFunction::Occurence,
            ankerl::unordered_dense::map<key_t, PairGroupBy<UIntT>>,   
            std::conditional_t<
                !(std::is_same_v<TColVal, void>),
                ankerl::unordered_dense::map<key_t, PairGroupBy<element_type_t<TColVal>>>,
                std::variant<
                    std::monostate,
                    ankerl::unordered_dense::map<key_t, std::string>, 
                    ankerl::unordered_dense::map<key_t, CharT>, 
                    ankerl::unordered_dense::map<key_t, uint8_t>, 
                    ankerl::unordered_dense::map<key_t, IntT>, 
                    ankerl::unordered_dense::map<key_t, UIntT>, 
                    ankerl::unordered_dense::map<key_t, FloatT>,
                    ankerl::unordered_dense::map<key_t, ReservingVec<std::string>>, 
                    ankerl::unordered_dense::map<key_t, ReservingVec<CharT>>, 
                    ankerl::unordered_dense::map<key_t, ReservingVec<uint8_t>>, 
                    ankerl::unordered_dense::map<key_t, ReservingVec<IntT>>, 
                    ankerl::unordered_dense::map<key_t, ReservingVec<UIntT>>, 
                    ankerl::unordered_dense::map<key_t, ReservingVec<FloatT>>
                >
            >
        >
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
    unsigned int idx_type                   = key_table_build<TContainer>(var_key_table, x);
    const std::vector<unsigned int> key_idx = idx_build_sametype<MapCol>(x, idx_type);

    using val_variant_t = std::conditional_t<
                              !std::is_same_v<TColVal, void>,
                              const std::vector<std::vector<element_type_t<TColVal>>>*,
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
    val_variant_t var_val_table;
    size_t val_idx;
    unsigned int pre_idx_type;

    // all is taken by ref, appart from n
    val_table_build<element_type_t<TColVal>, 
                    Function, 
                    MapCol, 
                    F>(var_val_table,
                       idx_type, 
                       val_idx, 
                       pre_idx_type, 
                       n);

    map_t var_lookup;
    if constexpr (std::is_same_v<TColVal, void>) {
        if constexpr  (Function != GroupFunction::Gather) {
            switch (pre_idx_type) {
                case 0: var_lookup.emplace<1>(); break;
                case 1: var_lookup.emplace<2>(); break;
                case 2: var_lookup.emplace<3>(); break;
                case 3: var_lookup.emplace<4>(); break;
                case 4: var_lookup.emplace<5>(); break;
                case 5: var_lookup.emplace<6>(); break;
            }
        } else {
            switch (pre_idx_type) {
                case 0: var_lookup.emplace<7>(); break;
                case 1: var_lookup.emplace<8>(); break;
                case 2: var_lookup.emplace<9>(); break;
                case 3: var_lookup.emplace<10>(); break;
                case 4: var_lookup.emplace<11>(); break;
                case 5: var_lookup.emplace<12>(); break;
            }
        }
    }

    constexpr auto& size_table = get_types_size();
    const size_t val_size      = size_table[idx_type];

    std::vector<std::string_key*> key_vec(local_nrow);

    if constexpr (CORES == 1) {
        if constexpr (Function == GroupFunction::Occurence) {

            dispatch1_onecol<Function, 
                             0, // normal mode
                             element_type_t<TContainer>, 
                             element_type_t<TColVal>,
                             OccLookupSameDiffType,
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

        } else if constexpr (Function == GroupFunction::Sum ||
                             Function == GroupFunction::Mean) {

            dispatch1_onecol<Function, 
                             0, // normal mode
                             element_type_t<TContainer>,
                             element_type_t<TColVal>,
                             AddLookupSameDiffType,
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

        } else {

            dispatch1_onecol<Function, 
                             0, // normal mode
                             element_type_t<TContainer>,
                             element_type_t<TColVal>,
                             FillLookupSameDiffType,
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

        }

    } else if constexpr (CORES > 1) {

        const bool triv_copy = (idx_type != 0);
        const unsigned int chunks = local_nrow / CORES + 1;
        std::vector<map_t> vec_map(CORES);

        #pragma omp parallel num_threads(CORES)
        {
            const unsigned int tid   = omp_get_thread_num();
            const unsigned int start = tid * chunks;
            const unsigned int end   = std::min(local_nrow, start + chunks);
            map_t& cur_map           = vec_map[tid];

            if constexpr (Function == GroupFunction::Occurence) {

                dispatch1_onecol<Function, 
                                 0, // normal mode
                                 element_type_t<TContainer>,
                                 element_type_t<TColVal>,
                                 OccLookupSameDiffType,
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

            } else if constexpr (Function == GroupFunction::Sum ||
                                 Function == GroupFunction::Mean) {

                dispatch1_onecol<Function, 
                                 0, // normal mode
                                 element_type_t<TContainer>,
                                 element_type_t<TColVal>,
                                 AddLookupSameDiffType,
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

            } else {

                dispatch1_onecol<Function, 
                                 0, // normal mode
                                 element_type_t<TContainer>,
                                 element_type_t<TColVal>,
                                 FillLookupSameDiffType,
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
        }

        if (triv_copy) {
            dispatch_merge<Function, 
                           MergeCurMapTriv,
                           element_type_t<TColVal>>(vec_map, 
                                                    var_lookup,
                                                    NPerGroup,
                                                    val_table);
        } else {
            dispatch_merge<Function, 
                           MergeCurMap,
                           element_type_t<TColVal>>(vec_map,
                                                    var_lookup,
                                                    NPerGroup,
                                                    val_table);
        }

    }

    col_value_t var_v_col;
    if constexpr (!RsltTypeKnown) { 
        switch (idx_type) {
            case 0: var_v_col.emplace<1>(); break;
            case 1: var_v_col.emplace<2>(); break;
            case 2: var_v_col.emplace<3>(); break;
            case 3: var_v_col.emplace<4>(); break;
            case 4: var_v_col.emplace<5>(); break;
            case 5: var_v_col.emplace<6>(); break;
        }
    }

    if (!in_view) {
        in_view = true;
        row_view_idx.resize(local_nrow);
        row_view_map.reserve(local_nrow);
        for (size_t i = 0; i < local_nrow; ++i)
            row_view_map.emplace(i, i);
    }

    dispatch_create_value_col<CreateValueCol, 
                              Function, 
                              CORES, 
                              RsltTypeKnown,
                              StandardMethod>(f, 
                                              var_v_col, 
                                              var_lookup,
                                              local_nrow);

    for (size_t i = 0; i < local_nrow; ++i)
        row_view_map[i] = row_view_idx[i];

    dispatch_add_col<RsltTypeKnown>(v_col);

    if (!name_v.empty())
        name_v.push_back(colname);

    ++ncol;

}


