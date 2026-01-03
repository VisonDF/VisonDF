#pragma once

template <typename TColVal,
          GroupFunction Function>
inline void val_build_alrd_hard(auto& var_vec_grp,
                                const size_t n,
                                const size_t unique_grps
                               )
{
    if constexpr (std::is_same_v<TColVal, void>) {

        if constexpr (Function != GroupFunction::Gather) {

            switch (type_refv[n]) {
                case 's': var_vec_grp.emplace<std::vector<PairGroupBy<std::string>>>(unique_grps, 
                                              PairGroupBy<std::string>(NPerGroup)); break;

                case 'c': var_vec_grp.emplace<std::vector<PairGroupBy<CharT>>>(unique_grps,       
                                              PairGroupBy<CharT>(NPerGroup);        break;

                case 'b': var_vec_grp.emplace<std::vector<PairGroupBy<uint8_t>>>(unique_grps,     
                                              PairGroupBy<uint8_t>(NPerGroup));     break;

                case 'i': var_vec_grp.emplace<std::vector<PairGroupBy<IntT>>>(unique_grps,        
                                              PairGroupBy<IntT>(NPerGroup));        break;

                case 'u': var_vec_grp.emplace<std::vector<PairGroupBy<UIntT>>>(unique_grps, 	  
                                              PairGroupBy<UIntT>(NPerGroup));       break;

                case 'd': var_vec_grp.emplace<std::vector<PairGroupBy<FloatT>>>(unique_grps, 	  
                                              PairGroupBy<FloatT>(NPerGroup));      break;
            }

        } else {

            switch (type_refv[n]) {
                case 's': var_vec_grp.emplace<std::vector<PairGroupBy<ReservingVec<std::string>>>>(unique_grps,
            			          				          PairGroupBy<ReservingVec<std::string>>(NPerGroup)); break;

                case 'c': var_vec_grp.emplace<std::vector<PairGroupBy<ReservingVec<CharT>>>>(unique_grps,
            			          				          PairGroupBy<ReservingVec<CharT>>(NPerGroup));       break;

                case 'b': var_vec_grp.emplace<std::vector<PairGroupBy<ReservingVec<uint8_t>>>>(unique_grps,
            			          				          PairGroupBy<ReservingVec<uint8_t>>(NPerGroup));     break;

                case 'i': var_vec_grp.emplace<std::vector<PairGroupBy<ReservingVec<IntT>>>>(unique_grps,
            			          				          PairGroupBy<ReservingVec<IntT>>(NPerGroup));        break;

                case 'u': var_vec_grp.emplace<std::vector<PairGroupBy<ReservingVec<UIntT>>>>(unique_grps,
            			          				          PairGroupBy<ReservingVec<UIntT>>(NPerGroup));       break;

                case 'd': var_vec_grp.emplace<std::vector<PairGroupBy<ReservingVec<FloatT>>>>(unique_grps,
            			      				              PairGroupBy<ReservingVec<FloatT>>(NPerGroup));      break;
            }

        }

    } else {

	    if constexpr (Function == GroupFunction::Gather) {

	        var_vec_grp = std::vector<PairGroupBy<ReservingVec<element_type_t<TColVal>>>>(unique_grps, 
                                      PairGroupBy<ReservingVec<element_type_t<TColVal>>>(NPerGroup));

	    } else {

	        var_vec_grp = std::vector<PairGroupBy<element_type_t<TColVal>>>(unique_grps, 
                                      PairGroupBy<element_type_t<TColVal>>(NPerGroup));

	    }
    }
}







