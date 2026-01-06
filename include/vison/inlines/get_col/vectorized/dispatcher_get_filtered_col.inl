#pragma once

template <unsigned int Size,
          typename T>
inline void dispatch_get_filtered_col(
                                     const std::vector<T>& col_vec,
                                     std::vector<T>& rtn_v,
                                     const std::vector<uint8_t>& mask,
                                     const unsigned int strt_vl,
                                     const size_t strt,
                                     const size_t end,
                                     const size_t out_idx_vl
                                     )
{

    if constexpr (size == 1) {
       
        get_filtered_col_8(
                           col_vec, 
                           rtn_v,
                           mask,
                           strt_vl,
                           strt,
                           end,
                           out_idx_vl
                           );

    } else if constexpr (size == 2) {

        get_filtered_col_16(
                            col_vec, 
                            rtn_v,
                            mask,
                            strt_vl,
                            strt,
                            end,
                            out_idx_vl
                            );

    } else if constexpr (size == 4) {

        get_filtered_col_32(
                            col_vec, 
                            rtn_v,
                            mask,
                            strt_vl,
                            strt,
                            end,
                            out_idx_vl
                            );

    } else if constexpr (size == 8) {

        get_filtered_col_64(
                            col_vec, 
                            rtn_v,
                            mask,
                            strt_vl,
                            strt,
                            end,
                            out_idx_vl
                            );

    }

}



