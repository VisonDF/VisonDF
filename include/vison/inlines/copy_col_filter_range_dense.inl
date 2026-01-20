#pragma once

template <bool OneIsTrue,
          bool Periodic,
          bool Distinct,
          typename T
         >
inline void copy_col_filter_range_dense(
                                         T* dst,
                                         const T* src,
                                         const std::vector<uint8_t>& mask,
                                         size_t i,
                                         size_t out_idx,
                                         const size_t cur_start,
                                         const size_t cur_end,
                                         const size_t n_el2
                                       )
{

    if constexpr (!Periodic) {
        if constexpr (OneIsTrue) {
            while (!mask[i]) {
                i += 1;
                out_idx += 1;
            }
        } else {
            while (mask[i]) {
                i += 1;
                out_idx += 1;
            }
        }
        while (i < cur_end) {

            if constexpr (OneIsTrue) {
                while (i < cur_end && mask[i]) {
                    i += 1;
                }
            } else {
                while (i < cur_end && !mask[i]) {
                    i += 1;
                }
            }

            const size_t start = i;
            if constexpr (OneIsTrue) {
                while (i < cur_end && !mask[i]) ++i;
            } else {
                while (i < cur_end && mask[i]) ++i;
            }
        
            size_t len = i - start;
            {
                T* __restrict d = dst + out_idx;
                T* __restrict s = src + start;

                if constexpr (Distinct) {
                    memcpy(d, s, len * sizeof(T))
                } else {
                    memmove(d, s, len * sizeof(T))
                }

            }
        
            out_idx += len;
            i += 1;
        }
    } else {

        size_t k = cur_start % n_el2;

        if constexpr (OneIsTrue) {
            while (!mask[k]) {
                i += 1;
                k += 1;
                out_idx += 1;
                k -= (k == n_el2) * n_el2;
            }
        } else {
            while (mask[k]) {
                i += 1;
                k += 1;
                out_idx += 1;
                k -= (k == n_el2) * n_el2;
            }
        }
        while (i < cur_end) {

            if constexpr (OneIsTrue) {
                while (i < cur_end && mask[k]) {
                    i += 1;
                    k += 1;
                    k -= (k == n_el2) * n_el2;
                }
            } else {
                while (i < cur_end && !mask[k]) {
                    i += 1;
                    k += 1;
                    k -= (k == n_el2) * n_el2;
                }
            }

            const size_t start = i;
            if constexpr (OneIsTrue) {
                while (i < cur_end && !mask[k])  {
                    ++i;
                    k += 1;
                    k -= (k == n_el2) * n_el2;
                }
            } else {
                while (i < cur_end && mask[k])  { 
                    ++i;
                    k += 1;
                    k -= (k == n_el2) * n_el2;
                }
            }
        
            size_t len = i - start;
            {
                T* __restrict d = dst + out_idx;
                T* __restrict s = src + start;

                if constexpr (Distinct) {
                    memcpy(d, s, len * sizeof(T))
                } else {
                    memmove(d, s, len * sizeof(T))
                }

            }
        
            out_idx += len;
            i += 1;
        }
    }
}





