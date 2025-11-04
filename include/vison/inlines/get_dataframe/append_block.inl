#pragma once

template <typename VecT>
inline void append_block(std::vector<VecT>& dst,
                         const std::vector<VecT>& src,
                         size_t col_idx,
                         size_t nrow)
{

    const size_t start = col_idx * nrow;
    
    if constexpr (std::is_trivially_copyable_v<VecT>) {
        const size_t old_size = dst.size();
        dst.resize(old_size + nrow);
        std::memcpy(dst.data() + old_size,
                    src.data() + start,
                    nrow * sizeof(VecT));
    } else {
        dst.insert(dst.end(),
                   src.begin() + start,
                   src.begin() + start + nrow);
    }

}


