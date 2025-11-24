#pragma once

template <
    bool ASC, 
    unsigned int CORES = 4, 
    bool Simd = true
>
inline void sort_char_from_string(
    std::vector<size_t>& idx,
    const uint8_t* col,
    const size_t nrow,
    const size_t max_length 
)
{

    if constexpr (CORES > 1)
        radix_sort_charbuf_flat_mt<CORES, Simd>(col, 
                                                nrow, 
                                                max_length, 
                                                idx.data());
    else
        radix_sort_charbuf_flat<Simd>          (col, 
                                                nrow, 
                                                max_length,
                                                idx.data());

    if constexpr (!ASC)
        std::reverse(idx.begin(), idx.end());
}



