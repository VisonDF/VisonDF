#pragma once

template <typename TColVal = void,
	      unsigned int CORES = 4,
	      unsigned int NPerGroup = 4,
          bool MapCol = false,
	      GroupFunction Function == GroupFunction::Occurence,
	      bool SanityCheck = true>
void transform_group_by_hard_alrd_mt(unsigned int Id,
                                     unsigned int n,
                                     std::string& colname = "n")
{
    if constexpr (SanityCheck) {
        if (Id > grp_by_col.size()) {
	        std::cerr << "Can't perform this operation because no group on these/this col have been performed\n'";
	        return;
        }
    }

    constexpr bool RsltTypeKnown = (!(std::is_same_v<TColVal, void>) && 
                                     Function != GroupFunction::Gather);

    using value_t =  std::conditional_t<!std::is_same_v<TColVal, void>, 
	  			         std::conditional_t<
				         	Function == GroupFunction::Gather,
				         	PairGroupBy<ReservingVec<element_type_t<TColVal>>>,
				         	PairGroupBy<element_type_t<TColVal>>
                         >, 
				         std::variant<
			                 std::monostate,
                             std::vector<PairGroupBy<std::string>>, 
                             std::vector<PairGroupBy<CharT>>, 
                             std::vector<PairGroupBy<uint8_t>>, 
                             std::vector<PairGroupBy<IntT>>, 
                             std::vector<PairGroupBy<UIntT>>, 
                             std::vector<PairGroupBy<FloatT>>,
                             std::vector<PairGroupBy<ReservingVec<std::string>>>, 
                             std::vector<PairGroupBy<ReservingVec<CharT>>>, 
                             std::vector<PairGroupBy<ReservingVec<uint8_t>>>, 
                             std::vector<PairGroupBy<ReservingVec<IntT>>>, 
                             std::vector<PairGroupBy<ReservingVec<UIntT>>>, 
                             std::vector<PairGroupBy<ReservingVec<FloatT>>>
                         >
                     >;
    value_t var_val_grp;

    const char grp_by              = grp_by_col[Id];
    const unsigned int local_nrow  = nrow;
    const unsigned int unique_grps = unique_grp[Id];

    val_grp_build_alrd_hard<element_type_t<TColVal>, Function>(var_val_grp,
                                                               n,
                                                               unique_grps);

    using val_variant_t = std::conditional_t<
          !std::is_same_v<TColVal, void>,
          std::vector<element_type_t<TColVal>>*,
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
    val_variant_t var_val_table; // this serves just to call val_table_build

    unsigned int val_idx;
    unsigned int idx_type;
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

    if constexpr (CORES == 1) {

        if constexpr (Function == GroupFunction::Occurence) {

            dispatch_alrd<element_type_t<TColVal>>(occ_grp_by_alrd_hard,
                                                   0,
                                                   local_nrow,
                                                   val_idx,
                                                   grp_by,
                                                   var_val_grp); 

        } else if constexpr (Function == GroupFunctio::Sum ||
                             Function == GroupFunction::Mean) {

            dispatch_alrd<element_type_t<TColVal>>(add_grp_by_alrd_hard,
                                                   0,
                                                   local_nrow,
                                                   val_idx,
                                                   grp_by,
                                                   var_val_grp); 

        } else {

            dispatch_alrd<element_type_t<TColVal>>(fill_grp_by_alrd_hard,
                                                   0,
                                                   local_nrow,
                                                   val_idx,
                                                   grp_by,
                                                   var_val_grp); 

        }

    } else {
        const bool is_triv = (pre_idx_type != 0);
	    const unsigned int chunks = local_nrow / CORES + 1;
	    std::vector<value_t> var_val_grp_vec(chunks);

	    #pragma omp prallel num_threads(CORES)
	    {
            const unsigned int tid        = omp_get_thread_num();
            const unsigned int start      = tid * chunks;
            const unsigned int end        = std::min(local_nrow, start + chunks);
		    auto& cur_var_val_grp         = var_val_grp_vec[tid];
            val_grp_build_alrd<element_type_t<TColVal>, Function>(cur_var_val_grp,
                                                                  n,
                                                                  unique_grps);

            if constexpr (Function == GroupFunction::Occurence) {

                dispatch_alrd<element_type_t<TColVal>>(occ_grp_by_alrd_hard,
                                                       start
                                                       end,
                                                       val_idx,
                                                       grp_by,
                                                       cur_var_val_grp); 

            } else if constexpr (Function == GroupFunction::Sum ||
                                 Function == GroupFunction::Mean) {

                dispatch_alrd<element_type_t<TColVal>>(add_grp_by_alrd_hard,
                                                       start
                                                       end,
                                                       val_idx,
                                                       grp_by,
                                                       cur_var_val_grp); 

            } else {

                dispatch_alrd<element_type_t<TColVal>>(fill_grp_by_alrd_hard,
                                                       start
                                                       end,
                                                       val_idx,
                                                       grp_by,
                                                       cur_var_val_grp); 

            }
	    }

        if (triv_copy) {

            dispatch_merge_alrd<TColVal, 
                                CORES,
                                Function,
                                MergeAlrdTrivHard>(unique_grps,
                                                   chunks,
                                                   grp_by,
                                                   var_val_grp_vec,
                                                   var_val_grp);

        } else {

            dispatch_merge_alrd<TColVal, 
                                CORES,
                                Function,
                                MergeAlrdHard>(unique_grps,
                                                   chunks,
                                                   grp_by,
                                                   var_val_grp_vec,
                                                   var_val_grp);

        }

    }

    using value_col_t = std::conditional_t<Function == GroupFunction::Occurence,
        std::vector<UIntT>,
        std::conditional_t<
                    RsltTypeKnown,
                    std::vector<element_type_t<TColVal>>,
                    std::variant<
                        std::monostate,
                        std::vector<std::string>,
                        std::vector<CharT>,
                        std::vector<uint8_t>,
                        std::vector<IntT>,
                        std::vector<UIntT>,
                        std::vector<FloatT>,
        >>>;
    value_col_t var_value_col;

    if constexpr (!RsltTypeKnown) {
        switch (idx_type) {
            case 0: : var_value_col.emplace<1>() ;break; 
            case 1: : var_value_col.emplace<2>() ;break; 
            case 2: : var_value_col.emplace<3>() ;break; 
            case 3: : var_value_col.emplace<4>() ;break; 
            case 4: : var_value_col.emplace<5>() ;break; 
            case 5: : var_value_col.emplace<6>() ;break; 
        }
    }

    std::visit([](auto&& value_col) {

        using TP = std::decay_t<decltype(value_col)>;

        if constexpr (!std::is_same_v<TP, std::monostate>) {

            value_col.resize(local_nrow);

            using Elem = TP::value_type;
            const unsigned int val_size = sizeof(Elem);

            if constexpr (CORES > 1) {

                std::vector<size_t> pos_boundaries;
                pos_boundaries.reserve(unique_grps);
                pos_boundaries.push_back(0);
                
                for (size_t t = 0; t < unqiue_grps; ++t) {
                    pos_boundaries.push_back(
                        pos_boundaries.back() + vec_grp[t].size()
                    );
                }
                
                #pragma omp parallel for num_threads(CORES) schedule(static)
                for (size_t i = 0; i < unique_grps; ++i) {
                    size_t start           = pos_boundaries[i];
                    size_t len             = pos_boundaries[i + 1] - pos_boundaries[i];
                    const auto& cur_struct = val_grp[grp_by[i]];
                    const auto& val        = cur_struct.value;
                    const auto& vec        = cur_struct.idx_vec.v;

                    if constexpr (Function == GroupFunction::Occ ||
                                  Function == GroupFunction::Add) {

                        auto* __restrict__ out = value_col.data() + start;
                        auto const* __restrict__ in = &val; 
                        std::fill_n(out, len, *in);

                    } else if constexpr (Function == GroupFunction::Mean) {

                        const auto val2 = val / local_nrow;
                        auto* __restrict__ out = value_col.data() + start;
                        auto const* __restrict__ in = &val2; 
                        std::fill_n(out, len, *in);

                    } else {

                        const auto val2 = f(val.v);
                        auto* __restrict__ out = value_col.data() + start;
                        auto const* __restrict__ in = &val2; 
                        std::fill_n(out, len, *in);

                    }

                    memcpy(row_view_idx.data() + start,
                           vec.data(),
                           len * sizeof(unsigned int));
                }

            } else {

                size_t start = 0;
                for (size_t i = 0; i < unique_grps; ++i) {
                    const auto& cur_struct = val_grp[grp_by[i]];
                    const auto& val        = cur_struct.value;
                    const auto& vec        = cur_struct.idx_vec.v;
                    const size_t len       = vec.size();

                    if constexpr (Function == GroupFunction::Occ ||
                                  Function == GroupFunction::Add) {

                        auto* __restrict__ out = value_col.data() + start;
                        auto const* __restrict__ in = &val; 
                        std::fill_n(out, len, *in);

                    } else if constexpr (Function == GroupFunction::Mean) {

                        const auto val2 = val / local_nrow;
                        auto* __restrict__ out = value_col.data() + start;
                        auto const* __restrict__ in = &val2; 
                        std::fill_n(out, len, *in);

                    } else {

                        const auto val2 = f(val.v);
                        auto* __restrict__ out = value_col.data() + start;
                        auto const* __restrict__ in = &val2; 
                        std::fill_n(out, len, *in);

                    }

                    memcpy(row_view_idx.data() + start,
                           vec.data(),
                           len * sizeof(unsigned int));

                    start += len;

                }

            }

        }

    }, var_value_col);

    switch (idx_type) {
        case 0: type_refv.push_back('s'); str_v.push_back(value_col);  break;
        case 1: type_refv.push_back('c'); chr_v.push_back(value_col);  break;
        case 2: type_refv.push_back('b'); bool_v.push_back(value_col); break;
        case 3: type_refv.push_back('i'); int_v.push_back(value_col);  break;
        case 4: type_refv.push_back('u'); uint_v.push_back(value_col); break;
        case 5: type_refv.push_back('d'); dbl_v.push_back(value_col);  break;
    }

    if (!name_v.empty())
        name_v.push_back(colname);

    col_alrd_materialized.try_emplace(ncol);

     ++ncol;

}







