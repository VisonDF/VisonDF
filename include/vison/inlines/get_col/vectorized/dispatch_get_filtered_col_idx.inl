#pragma once

template <unsigned int Size,
          typename T>
inline void dispatch_get_filtered_col_idx(auto& col_vec,
                                          std::vector<T>& rtn_v,
                                          const std::vector<unsigned int>& mask,
                                          const size_t strt,
                                          const size_t end
        )
{

    if constexpr (Size == 1) {

        get_filtered_col_idx_8(col_vec,
                               rtn_v,
                               mask,
                               strt,
                               end);

    } else if constexpr (Size == 2) {

        get_filtered_col_idx_16(col_vec,
                                rtn_v,
                                mask,
                                strt,
                                end);

    } else if constexpr (Size == 4) {

        get_filtered_col_idx_232(col_vec,
                                rtn_v,
                                mask,
                                strt,
                                end);

    } else if constexpr (Size == 8) {

        get_filtered_col_idx_64(col_vec,
                                rtn_v,
                                mask,
                                strt,
                                end);

    }

}




