#pragma once

template <typename TColVal = void,
	      unsigned int CORES = 4,
	      unsigned int NPerGroup = 4,
          bool MapCol = false,
          bool StandardMethod = false,
	      GroupFunction Function == GroupFunction::Occurence,
	      bool SanityCheck = true>
void transform_group_by_alrd_mt(unsigned int Id,
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
                             std::vector<std::string>, 
                             std::vector<CharT>, 
                             std::vector<uint8_t>, 
                             std::vector<IntT>, 
                             std::vector<UIntT>, 
                             std::vector<FloatT>,
                             std::vector<ReservingVec<std::string>>, 
                             std::vector<ReservingVec<CharT>>, 
                             std::vector<ReservingVec<uint8_t>>, 
                             std::vector<ReservingVec<IntT>>, 
                             std::vector<ReservingVec<UIntT>>, 
                             std::vector<ReservingVec<FloatT>>
                         >
                     >;
    value_t var_val_grp;

    const char grp_by              = grp_by_col[Id];
    const unsigned int local_nrow  = nrow;
    const unsigned int unique_grps = unique_grp[Id];

    val_grp_build_alrd<element_type_t<TColVal>, Function>(var_val_grp,
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
    val_variant_t var_val_table;

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

            dispatch_alrd<element_type_t<TColVal>>(occ_grp_by_alrd,
                                                   0,
                                                   local_nrow,
                                                   val_idx,
                                                   grp_by,
                                                   var_val_grp,
                                                   var_val_table); 

        } else if constexpr (Function == GroupFunctio::Sum ||
                             Function == GroupFunction::Mean) {

            dispatch_alrd<element_type_t<TColVal>>(add_grp_by_alrd,
                                                   0,
                                                   local_nrow,
                                                   val_idx,
                                                   grp_by,
                                                   var_val_grp,
                                                   var_val_table); 

        } else {

            dispatch_alrd<element_type_t<TColVal>>(fill_grp_by_alrd,
                                                   0,
                                                   local_nrow,
                                                   val_idx,
                                                   grp_by,
                                                   var_val_grp,
                                                   var_val_table); 

        }

    } else {
	    const unsigned int chunks = local_nrow / CORES + 1;
	    std::vector<var_variant_t> var_val_grp_vec(chunks);

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

                dispatch_alrd<element_type_t<TColVal>>(occ_grp_by_alrd,
                                                       start
                                                       end,
                                                       val_idx,
                                                       grp_by,
                                                       cur_var_val_grp,
                                                       var_val_table); 

            } else if constexpr (Function == GroupFunction::Sum ||
                                 Function == GroupFunction::Mean) {

                dispatch_alrd<element_type_t<TColVal>>(add_grp_by_alrd,
                                                       start
                                                       end,
                                                       val_idx,
                                                       grp_by,
                                                       cur_var_val_grp,
                                                       var_val_table); 

            } else {

                dispatch_alrd<element_type_t<TColVal>>(fill_grp_by_alrd,
                                                       start
                                                       end,
                                                       val_idx,
                                                       grp_by,
                                                       cur_var_val_grp,
                                                       var_val_table); 

            }
	    }

        dispatch_merge_alrd<TColVal, 
                            CORES,
                            Function,
                            MergeAlrd>(unique_grps,
                                      chunks,
                                      grp_by,
                                      var_val_grp_vec,
                                      var_val_grp);

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

            if constexpr (Function == GroupFunction::Occ ||
                          Function == GroupFunction::Add)

                #pragma omp parallel for if(CORES > 1) schedule(static)
                for (size_t i = 0; i < grp_by.size(); ++i) {
                    value_col[i] = val_grp[grp_by[i]];

            } else if constexpr (Function == GroupFunction::Mean) {

                for (auto& el : val_grp)
                    el / local_nrow;

                #pragma omp parallel for if(CORES > 1) schedule(static)
                for (size_t i = 0; i < grp_by.size(); ++i) {
                    value_col[i] = val_grp[grp_by[i]];

            } else {

                using TP2 = std::vector<element_type_t<TP::value_type>>;
                TP2 val_grp2;

                if constexpr (!StandardMethod) {

                    val_grp2.resize(unique_grps);
                    for (size_t i = 0; i < unique_grps; ++i)
                        val_grp2[i] = f(val_grp[i].v);

                    #pragma omp parallel for if(CORES > 1) schedule(static)
                    for (size_t i = 0; i < local_nrow; ++i) {
                        value_col[i] = val_grp2[grp_by[i]];

                } else {

                    #pragma omp parallel for if(CORES > 1) schedule(static)
                    for (size_t i = 0; i < local_nrow; ++i) {
                        value_col[i] = f(val_grp[grp_by[i]].v);

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

     ++ncol;

}







