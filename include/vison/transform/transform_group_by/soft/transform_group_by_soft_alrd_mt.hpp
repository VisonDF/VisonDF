#pragma once

template <unsigned int Id,
	  unsigned int CORES = 4,
	  unsigned int NPerGroup = 4,
	  bool SanityCheck = true>
void transform_group_by_soft_alrd_mt()
{
    if constexpr (SanityCheck) {
      if (Id > grp_by_col.size()) {
	std::cerr << "Can't perform this operation because no group on these/this col have been performed\n'";
	return;
      }
    }

    auto& grb_by_vl = grp_by_col[Id];
    const unsigned int local_nrow = nrow;
    const unsigned int unique_grps = unique_grp[Id];
    std::vector<std:vector<unsigned int>> vec_grp(unique_grps);
    for (auto& el: vec_grp)
        el.reserve(NPerGroup);

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







