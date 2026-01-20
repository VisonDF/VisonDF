#pragma once

template <bool IdxIsTrue,
          bool Periodic,
          bool Distinct,
          typename T
         >
inline void copy_col_filter_idx_dense(
                                       T* dst,
                                       const T* src,
                                       Runs& runs,
                                       const size_t delta,
                                       const size_t delta2,
                                       const size_t cur_start,
                                       const size_t cur_end
                                     )
{

    if constexpr (Distinct) {

        if constexpr (IdxIsTrue) {
            for (size_t r = cur_start; r < cur_end; ++r) {
                const auto& run = runs.vec[delta + r]; 
                std::memcpy(dst.data() + run.mask_pos,
                            src.data() + run.src_start,
                            run.len * sizeof(T));
            }
        } else {
            for (size_t r = cur_start; r < cur_end; ++r) {
                const auto& run = runs[delta + r]; 
                std::memcpy(dst.data() + delta2 + run.mask_pos,
                            src.data() + run.src_start,
                            run.len * sizeof(T));
            }
        }

    } else {

        if constexpr (IdxIsTrue) {
            for (size_t r = cur_start; r < cur_end; ++r) {
                const auto& run = runs.vec[delta + r]; 
                std::memmove(dst.data() + run.mask_pos,
                             src.data() + run.src_start,
                             run.len * sizeof(T));
            }
        } else {
            for (size_t r = cur_start; r < cur_end; ++r) {
                const auto& run = runs[delta + r]; 
                std::memmove(dst.data() + delta2 + run.mask_pos,
                             src.data() + run.src_start,
                             run.len * sizeof(T));
            }
        }

    }
}





