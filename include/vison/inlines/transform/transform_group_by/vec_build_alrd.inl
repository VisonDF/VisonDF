#pragma once

template <typename TColVal,
          GroupFunction Function>
inline void vec_build_alrd(auto& var_vec_grp,
                           const size_t n,
                           const size_t unique_grps
                          )
{
    if constexpr (std::is_same_v<TColVal, void>) {

        if constexpr (Function != GroupFunction::Gather) {

            switch (type_refv[n]) {
                case 's': var_vec_grp.emplace<std::vector<std::string>>(unique_grps, std::string{}>); break;
                case 'c': var_vec_grp.emplace<std::vector<CharT>>(unique_grps,       CharT{}>);       break;
                case 'b': var_vec_grp.emplace<std::vector<uint8_t>>(unique_grps,     uint8_t{}>);     break;
                case 'i': var_vec_grp.emplace<std::vector<IntT>>(unique_grps,        IntT{}>);        break;
                case 'u': var_vec_grp.emplace<std::vector<UIntT>>(unique_grps, 	     UIntT{}>);       break;
                case 'd': var_vec_grp.emplace<std::vector<FloatT>>(unique_grps, 	 FloatT{}>);      break;
            }

        } else {

            switch (type_refv[n]) {
                case 's': var_vec_grp.emplace<std::vector<ReservingVec<std::string>>>(unique_grps,
            			          				          ReservingVec<std::string>>(NPerGroup)); break;
                case 'c': var_vec_grp.emplace<std::vector<ReservingVec<CharT>>>(unique_grps,
            			          				          ReservingVec<CharT>>(NPerGroup));       break;
                case 'b': var_vec_grp.emplace<std::vector<ReservingVec<uint8_t>>>(unique_grps,
            			          				          ReservingVec<uint8_t>>(NPerGroup));     break;
                case 'i': var_vec_grp.emplace<std::vector<ReservingVec<IntT>>>(unique_grps,
            			          				          ReservingVec<IntT>>(NPerGroup));        break;
                case 'u': var_vec_grp.emplace<std::vector<ReservingVec<UIntT>>>(unique_grps,
            			          				          ReservingVec<UIntT>>(NPerGroup));       break;
                case 'd': var_vec_grp.emplace<std::vector<ReservingVec<FloatT>>>(unique_grps,
            			      				              ReservingVec<FloatT>>(NPerGroup));      break;
            }

        }

    } else {

	    if constexpr (Function == GroupFunction::Gather) {

	        var_vec_grp = std::vector<ReservingVec<element_type_t<TColVal>>>(unique_grps, 
                                                                             ReservingVec<element_type_t<TColVal>>(NPerGroup));

	    } else {

	        var_vec_grp = std::vector<element_type_t<TColVal>>(unique_grps, element_type_t<TColVal>{});

	    }
    }
}




