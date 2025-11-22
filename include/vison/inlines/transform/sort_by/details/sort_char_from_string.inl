#pragma once

template <
    bool ASC, 
    unsigned int CORES = 4, 
    bool Simd = true
>
inline void sort_char_from_string(
    std::vector<size_t>& idx,
    const uint8_t* col,
    size_t nrow
)
{

    if constexpr (CORES > 1)
        radix_sort_charbuf_flat_mt<CORES, Simd>(col, 
                                                nrow, 
                                                df_charbuf_size, 
                                                idx.data());
    else
        radix_sort_charbuf_flat<Simd>          (col, 
                                                nrow, 
                                                df_charbuf_size,
                                                idx.data());

    if constexpr (!ASC)
        std::reverse(idx.begin(), idx.end());
}



