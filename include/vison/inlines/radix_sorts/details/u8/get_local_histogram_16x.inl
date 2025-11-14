#pragma once

inline size_t* get_local_histogram_16x_u8() {
    static thread_local std::array<size_t, RADIX_LANES_AVX512 * RADIX_KI8> local{};
    return local.data();
}
