#pragma once

template <unsigned int Id,
	  typename TColVal = void,
	  unsigned int CORES = 4,
	  unsigned int NPerGroup = 4,
	  GroupFunction Function == GroupFunction::Occurence,
	  bool SanityCheck = true>
void transform_group_by_hard_alrd_mt()
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
            			      				       PairGroupBy<CharT>(NPerGroup)); break;
                case 'b': vec_grp.emplace<std::vector<PairGroupBy<uint8_t>>>(unique_grps,     
            			      					 PairGroupBy<uint8_t>(NPerGroup)); break;
                case 'i': vec_grp.emplace<std::vector<PairGroupBy<IntT>>>(unique_grps, 	  
            			      				      PairGroupBy<IntT>(NPerGroup)); break;
                case 'u': vec_grp.emplace<std::vector<PairGroupBy<UIntT>>>(unique_grps, 	  
            			      				       PairGroupBy<UIntT>(NPerGroup)); break;
                case 'd': vec_grp.emplace<std::vector<PairGroupBy<FloatT>>>(unique_grps, 	  
            			      					PairGroupBy<FloatT>(NPerGroup)); break;
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
            			      				PairGroupBy<ReservingVec<UIntT>>(NPerGroup));        break;
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

    if constexpr (CORES == 1) {
        for (size_t i = 0; i < local_nrow; ++i)
            vec_grp[grp_by_vl[i]].push_back(i);        
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
                for (auto& el: cur_grp_by)
            	    el.reserve(NPerGroup);
                for (size_t i = 0; i < local_nrow; ++i)
                    cur_grp_by[grp_by_vl[i]].push_back(i);
	    }
	    #pragma omp parallel for num_threads(CORES)
            for (size_t i = 0; i < unique_grps; ++i) {
		size_t sz = 0;
	        for (size_t i2 = 0; i2 < chunks; ++i2) {
		   const auto& cur_grp_by      = grp_by_cols[i2][i];
		   const unsigned int cur_size = cur_grp_by.size();
		   memcpy(vec_grp.data() + sz, 
			  cur_grp_by.data(), 
			  cur_grp_by_size * sizeof(unsigned int)); 
		   sz += cur_size;
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
        for (size_t i = 0; i < lookup.size(); ++i) {
            const auto& pos_vec = vec_grp[i];
            memcpy(row_view_idx.data() + i2, 
                   pos_vec.data(), 
                   sizeof(unsigned int) * pos_vec.size());
            i2 += pos_vec.size();
        }
    }

}







