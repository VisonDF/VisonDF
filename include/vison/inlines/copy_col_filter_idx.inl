#pragma once

template <bool IdxIsTrue,
          bool Periodic,
          bool Copy
         >
inline void copy_col_filter_idx(
                                auto* dst,
                                const auto* src,
                                const std::vector<size_t>& mask,
                                size_t out_idx,
                                const size_t cur_start,
                                const size_t cur_end,
                                const size_t n_el2
                               )
{

    if constexpr (Copy) {
        if constexpr (!Periodic) {
            if constexpr (IdxIsTrue) {
                for (size_t i = cur_start; i < cur_end; ++i)
                    dst[i] = src[mask[i]];
            } else {
                for (size_t i = cur_start; i < cur_end; ++i) {
                    const size_t next_stop = mask[i];
                    while (out_idx < next_stop) {
                        dst[out_idx] = src[out_idx];
                        out_idx += 1;
                    }
                }
            }
        } else {
            size_t k = cur_start % n_el2;
            if constexpr (IdxIsTrue) {
                for (size_t i = cur_start; i < cur_end; ++i) {
                    dst[i] = src[mask[k]];
                    k += 1;
                    k -= (k == n_el2) * n_el2;
                }
            } else {
                size_t out_idx2 = (k > 0) ? mask[k - 1] + 1 : 0;
                for (size_t i = cur_start; i < cur_end; ++i) {
                    while (out_idx2 < mask[k]) {
                        dst[out_idx] = src[out_idx++];
                        out_idx2 += 1;
                    }
                    i        += 1;
                    out_idx2 += 1;
                    out_idx2 -= (k == n_el2) * out_idx2;
                    k += 1;
                    k -= (k == n_el2) * n_el2;
                }
            }
        }
    } else {
        if constexpr (!Periodic) {
            if constexpr (IdxIsTrue) {
                for (size_t i = cur_start; i < cur_end; ++i)
                    dst[i] = std::move(src[mask[i]]);
            } else {
                for (size_t i = cur_start; i < cur_end; ++i) {
                    const size_t next_stop = mask[i];
                    while (out_idx < next_stop) {
                        dst[out_idx] = std::move(src[out_idx]);
                        out_idx += 1;
                    }
                }
            }
        } else {
            size_t k = cur_start % n_el2;
            if constexpr (IdxIsTrue) {
                for (size_t i = cur_start; i < cur_end; ++i) {
                    dst[i] = std::move(src[mask[k]]);
                    k += 1;
                    k -= (k == n_el2) * n_el2;
                }
            } else {
                size_t out_idx2 = (k > 0) ? mask[k - 1] + 1 : 0;
                for (size_t i = cur_start; i < cur_end; ++i) {
                    while (out_idx2 < mask[k]) {
                        dst[out_idx] = std::move(src[out_idx++]);
                        out_idx2 += 1;
                    }
                    i        += 1;
                    out_idx2 += 1;
                    out_idx2 -= (k == n_el2) * out_idx2;
                    k += 1;
                    k -= (k == n_el2) * n_el2;
                }
            }
        }
    }
}





