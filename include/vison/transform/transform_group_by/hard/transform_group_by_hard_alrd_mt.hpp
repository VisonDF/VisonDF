#pragma once

template <unsigned int Id,
	      typename TColVal = void,
	      unsigned int CORES = 4,
	      unsigned int NPerGroup = 4,
	      GroupFunction Function == GroupFunction::Occurence,
	      bool SanityCheck = true>
void transform_group_by_hard_alrd_mt(unsigned int n,
                                     std::string& colname = "n")
{
    if constexpr (SanityCheck) {
        if (Id > grp_by_col.size()) {
	        std::cerr << "Can't perform this operation because no group on these/this col have been performed\n'";
	        return;
        }
    }

    using value_t =  std::conditional_t<!std::is_same_v<TColVal, void>, 
	  			std::conditional_t<
					Function == GroupFunction::Gather,
					PairGroupBy<ReservingVec<element_type_t<TColVal>>>,
					PairGroupBy<element_type_t<TColVal>>>, 
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
                                >>;

    auto& grb_by_vl = grp_by_col[Id];
    const unsigned int local_nrow = nrow;
    const unsigned int unique_grps = unique_grp[Id];
    value_t vec_grp;
    if constexpr (std::is_same_v<TColVal, void>) {
        if constexpr (Function != GroupFunction::Gather) {
            switch (type_refv[grp_by_col[0]]) {
                case 's': vec_grp.emplace<std::vector<PairGroupBy<std::string>>>(unique_grps, 
            			      					      PairGroupBy<std::string>(NPerGroup)); break;
                case 'c': vec_grp.emplace<std::vector<PairGroupBy<CharT>>>(unique_grps,       
            			      				          PairGroupBy<CharT>(NPerGroup));       break;
                case 'b': vec_grp.emplace<std::vector<PairGroupBy<uint8_t>>>(unique_grps,     
            			      					      PairGroupBy<uint8_t>(NPerGroup));     break;
                case 'i': vec_grp.emplace<std::vector<PairGroupBy<IntT>>>(unique_grps, 	  
            			      				          PairGroupBy<IntT>(NPerGroup));        break;
                case 'u': vec_grp.emplace<std::vector<PairGroupBy<UIntT>>>(unique_grps, 	  
            			      				          PairGroupBy<UIntT>(NPerGroup));       break;
                case 'd': vec_grp.emplace<std::vector<PairGroupBy<FloatT>>>(unique_grps, 	  
            			      					      PairGroupBy<FloatT>(NPerGroup));      break;
            }
        } else {
            switch (type_refv[grp_by_col[0]]) {
                case 's': vec_grp.emplace<std::vector<PairGroupBy<ReservingVec<std::string>>>>(unique_grps,
            			      				          PairGroupBy<ReservingVec<std::string>>(NPerGroup)); break;
                case 'c': vec_grp.emplace<std::vector<PairGroupBy<ReservingVec<CharT>>>>(unique_grps,
            			      				          PairGroupBy<ReservingVec<CharT>>(NPerGroup));       break;
                case 'b': vec_grp.emplace<std::vector<PairGroupBy<ReservingVec<uint8_t>>>>(unique_grps,
            			      				          PairGroupBy<ReservingVec<uint8_t>>(NPerGroup));     break;
                case 'i': vec_grp.emplace<std::vector<PairGroupBy<ReservingVec<IntT>>>>(unique_grps,
            			      				          PairGroupBy<ReservingVec<IntT>>(NPerGroup));        break;
                case 'u': vec_grp.emplace<std::vector<PairGroupBy<ReservingVec<UIntT>>>>(unique_grps,
            			      				          PairGroupBy<ReservingVec<UIntT>>(NPerGroup));       break;
                case 'd': vec_grp.emplace<std::vector<PairGroupBy<ReservingVec<FloatT>>>>(unique_grps,
            			      				          PairGroupBy<ReservingVec<FloatT>>(NPerGroup));      break;
            }
        }
    } else {
	    if constexpr (Function == GroupFunction::Gather) {
	        vec_grp = std::vector<PairGroupBy<TColVal>>(unique_grps, 
	    		    			        PairGroupBy<ReservingVec<element_type_t<TColVal>>>(NPerGroup));
	    } else {
            vec_grp = std::vector<PairGroupBy<TColVal>>(unique_grps, PairGroupBy<element_type_t<TColVal>>(NPerGroup));
	    }
    }

    using key_variant_t = std::variant<
        std::nullptr_t,
        const std::vector<std::vector<std::string>>*,
        const std::vector<std::vector<CharT>>*,
        const std::vector<std::vector<uint8_t>>*,
        const std::vector<std::vector<IntT>>*,
        const std::vector<std::vector<UIntT>>*,
        const std::vector<std::vector<FloatT>>*
    >; 
    key_variant_t val_table = nullptr;

    unsigned int idx_type;
    if constexpr (!std::is_same_v<TColVal, void>) {
        if constexpr (std::is_same_v<TColVal, std::string>) {
            val_table = &str_v;
            idx_type = 0;
        } else if constexpr (std::is_same_v<TColVal, CharT>) {
            val_table = &chr_v;
            idx_type = 1;
        } else if constexpr (std::is_same_v<TColVal, uint8_t>) {
            val_table = &bool_v;
            idx_type = 2;
        } else if constexpr (std::is_same_v<TColVal, IntT>) {
            val_table = &int_v;
            idx_type = 3;
        } else if constexpr (std::is_same_v<TColVal, UIntT>) {
            val_table = &uint_v;
            idx_type = 4;
        } else if constexpr (std::is_same_v<TColVal, FloatT>) {
            val_table = &dbl_v;
            idx_type = 5;
        }
    } else {
        switch (type_refv[n]) {
            case 's': val_table = &str_v ; idx_type = 0; break;
            case 'c': val_table = &chr_v ; idx_type = 1; break;
            case 'b': val_table = &bool_v ; idx_type = 2; break;
            case 'i': val_table = &int_v ; idx_type = 3; break;
            case 'u': val_table = &uint_v ; idx_type = 4; break;
            case 'd': val_table = &dbl_v ; idx_type = 5; break;
        }
    }

    if constexpr (CORES == 1) {
        if constexpr (Function == GroupFunction::Occurence) {
            for (size_t i = 0; i < local_nrow; ++i) {
                vec_grp[grp_by_vl[i]].idx_vec.push_back(i);
                ++vec_grp[grp_by_vl[i]].value;
            }
        } else if constexpr (Function == GroupFunctio::Sum ||
                             Function == GroupFunction::Mean) {
            for (size_t i = 0; i < local_nrow; ++i) {
                vec_grp[grp_by_vl[i]].idx_vec.push_back(i);
                vec_grp[grp_by_vl[i]].value += val_table[n_col_real];
            }
        } else {
            for (size_t i = 0; i < local_nrow; ++i) {
                vec_grp[grp_by_vl[i]].idx_vec.push_back(i);
                vec_grp[grp_by_vl[i]].value.push_back(val_table[n_col_real]);
            }
        }
    } else {
	    const unsigned int chunks = local_nrow / CORES + 1;
	    std::vector<std::vector<std::vector<unsigned int>>> grp_by_cols(chunks,
			     						    std::vector<unsigned int>(unique_grps));
	    #pragma omp prallel num_threads(CORES)
	    {
            const unsigned int tid   = omp_get_thread_num();
            const unsigned int start = tid * chunks;
            const unsigned int end   = std::min(local_nrow, start + chunks);
		    auto& cur_grp_by         = grp_by_cols[tid];
            if constexpr (Function == GroupFunction::Occurence) {
                for (size_t i = start; i < end; ++i) {
                    cur_grp_by[grp_by_vl[i]].idx_vec.push_back(i);
                    ++cur_grp[grp_by_vl[i]].value;
                }
            } else if constexpr (Function == GroupFunction::Sum ||
                                 Function == GroupFunction::Mean) {
                for (size_t i = start; i < end; ++i) {
                    cur_grp_by[grp_by_vl[i]].idx_vec.push_back(i);
                    cur_grp[grp_by_vl[i]].value += val_table[n_col_real];
                }
            } else {
                for (size_t i = start; i < end; ++i) {
                    cur_grp_by[grp_by_vl[i]].idx_vec.push_back(i);
                    cur_grp[grp_by_vl[i]].value.push_back(val_table[n_col_real]);
                }
            }
	    }
	    #pragma omp parallel for num_threads(CORES)
        for (size_t i = 0; i < unique_grps; ++i) {
		    size_t sz = 0;
            for (size_t i2 = 0; i2 < chunks; ++i2) {
                const auto& cur_grp         = grp_by_cols[i2][i];
		        const auto& cur_grp_by      = cur_grp.idx_vec;
		        const unsigned int cur_size = cur_grp_by.size();
		        memcpy(vec_grp.data() + sz, 
		               cur_grp_by.data(), 
		               cur_size * sizeof(unsigned int)); 
		        sz += cur_size;
                const auto& cur_val = cur_grp.value;
                if constexpr (Function == GroupFunction::Occurence || 
                              Function == GroupFunction::Sum       ||
                              Function == GroupFunction::Mean) {
                    vec_grp[grp_by_vl[i]].value += cur_val;
                } else {
                    vec_grp[grp_by_vl[i]].value.push_back(cur_val);
                }
            }
	    }
    }
 
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
            size_t start    = pos_boundaries[i];
            size_t len      = pos_boundaries[i + 1] - pos_boundaries[i];
            const auto& vec = vec_grp[i];
            memcpy(row_view_idx.data() + start,
                   vec.data(),
                   len * sizeof(unsigned int));
        }
    } else {
        size_t i2 = 0;
        for (size_t i = 0; i < unique_grps; ++i) {
            const auto& pos_vec = vec_grp[i];
            memcpy(row_view_idx.data() + i2, 
                   pos_vec.data(), 
                   sizeof(unsigned int) * pos_vec.size());
            i2 += pos_vec.size();
        }
    }

}







