#pragma once

inline size_t* get_local_histogram_16x_u16() {
    static thread_local std::array<size_t, RADIX_LANES_AVX512 * RADIX_KI16> local{};
    return local.data();
}
