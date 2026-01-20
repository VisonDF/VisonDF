#pragma once

template <bool OneIsTrue,
          bool Periodic,
          bool Copy
         >
inline void copy_col_filter (
                             auto* dst,
                             const auto* src,
                             const std::vector<uint8_t>& mask,
                             size_t out_idx,
                             const size_t cur_start,
                             const size_t cur_end,
                             const size_t n_el2
                            )
{

    if constexpr (Copy) {
        if constexpr (!Periodic) {
            if constexpr (OneIsTrue) {
                for (size_t i = cur_start; i < cur_end; ++i) {
                    if (mask[i])
                        dst[out_idx++] = src[i];
                }
            } else {
                for (size_t i = cur_start; i < cur_end; ++i) {
                    if (!mask[i])
                        dst[out_idx++] = src[i];
                }
            }
        } else {
            if constexpr (OnsIsTrue) {
                for (size_t i = cur_start, k = cur_start % n_el2; i < cur_end; ++i) {
                    if (!mask[k]) { 
                        k += 1;
                        k -= (k == n_el2) * n_el2;
                        continue; 
                    }
                    dst[out_idx++] = src[i];
                }
            } else {
                for (size_t i = cur_start, k = cur_start % n_el2; i < cur_end; ++i) {
                    if (mask[k]) { 
                        k += 1;
                        k -= (k == n_el2) * n_el2;
                        continue; 
                    }
                    dst[out_idx++] = src[i];
                }
            }
        }
    } else {
        if constexpr (!Periodic) {
            if constexpr (OneIsTrue) {
                for (size_t i = cur_start; i < cur_end; ++i) {
                    if (mask[i])
                        dst[out_idx++] = std::move(src[i]);
                }
            } else {
                for (size_t i = cur_start; i < cur_end; ++i) {
                    if (!mask[i])
                        dst[out_idx++] = std::move(rc[i]);
                }
            }
        } else {
            if constexpr (OnsIsTrue) {
                for (size_t i = cur_start, k = cur_start % n_el2; i < cur_end; ++i) {
                    if (!mask[k]) { 
                        k += 1;
                        k -= (k == n_el2) * n_el2;
                        continue; 
                    }
                    dst[out_idx++] = std::move(src[i]);
                }
            } else {
                for (size_t i = cur_start, k = cur_start % n_el2; i < cur_end; ++i) {
                    if (mask[k]) { 
                        k += 1;
                        k -= (k == n_el2) * n_el2;
                        continue; 
                    }
                    dst[out_idx++] = std::move(src[i]);
                }
            }
        }
    }
}




