#pragma once

inline std::array<size_t, RADIX_KI16>& get_local_count_u16() {
    static thread_local std::array<size_t, RADIX_KI16> local;
    return local;
}

inline std::array<size_t, RADIX_KI16>& get_local_bucket_base_u16() {
    static thread_local std::array<size_t, RADIX_KI16> local;
    return local;
}

inline std::array<size_t, RADIX_KI16>& get_local_bucket_size_u16() {
    static thread_local std::array<size_t, RADIX_KI16> local;
    return local;
}

inline std::array<std::array<size_t, RADIX_KI16>, MAX_THREADS>& get_local_hist_u16() 
{
    static thread_local std::array<std::array<size_t, RADIX_KI16>, MAX_THREADS> local;
    return local;
}

inline std::array<std::array<size_t, RADIX_KI16>, MAX_THREADS>& get_local_thread_off_u16() 
{
    static thread_local std::array<std::array<size_t, RADIX_KI16>, MAX_THREADS> local;
    return local;
}

inline size_t* get_local_histogram_8x_u16()
{
    static thread_local std::array<size_t, RADIX_LANES * RADIX_KI16> local_hist;
    return local_hist.data();
}

inline size_t* get_local_histogram_16x_u16()
{
    static thread_local std::array<size_t, RADIX_LANES_AVX512 * RADIX_KI16> local_hist;
    return local_hist.data();
}






