#pragma once

template <unsigned int CORES = 4,
          unsigned int NPerGroup = 4,
          bool MapCol = false,
          bool StandardMethod = false,
	      bool SanityCheck = true>
void transform_group_by_soft_alrd_mt(unsigned int Id)
{
    if constexpr (SanityCheck) {
        if (Id > grp_by_col.size()) {
	        std::cerr << "Can't perform this operation because no group on these/this col have been performed\n'";
	        return;
        }
    }

    const char grp_by              = grp_by_col[Id];
    const unsigned int local_nrow  = nrow;
    const unsigned int unique_grps = unique_grp[Id];

    using value_t = ReservingVec<unsigned int>;
    value_t val_grp(unique_grps, ReservingVec<unsigned int>(NPerGroup));

    if constexpr (CORES == 1) {

        grp_by_alrd_soft(0,
                         local_nrow,
                         grp_by,
                         val_grp); 

    } else {
	    const unsigned int chunks = local_nrow / CORES + 1;
	    std::vector<var_variant_t> val_grp_vec(chunks);

	    #pragma omp prallel num_threads(CORES)
	    {
            const unsigned int tid        = omp_get_thread_num();
            const unsigned int start      = tid * chunks;
            const unsigned int end        = std::min(local_nrow, start + chunks);
		    auto& cur_var_val_grp         = var_val_grp_vec[tid];

            grp_by_alrd_soft(start,
                             end,
                             grp_by,
                             val_grp);

	    }

        merge_alrd_soft<CORES>(unique_grps,
                               chunks,
                               grp_by,
                               val_grp, 
                               val_grp_vec);

    }


    value_col.resize(local_nrow);

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
            const auto& vec        = val_grp[grp_by[i]].v;

            memcpy(row_view_idx.data() + start,
                   vec.data(),
                   len * sizeof(unsigned int));
        }

    } else {

        size_t start = 0;
        for (size_t i = 0; i < unique_grps; ++i) {
            const auto& vec        = val_grp[grp_by[i]].v;
            const size_t len       = vec.size();

            memcpy(row_view_idx.data() + start,
                   vec.data(),
                   len * sizeof(unsigned int));

            start += len;

        }

    }

}







