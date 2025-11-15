#pragma once

inline uint64_t double_to_sortable(double x) {
    uint64_t bits;
    memcpy(&bits, &x, sizeof(bits));

    uint64_t mask = -(bits >> 63);               // 0x000..0 if positive, 0xFFF..F if negative
    return bits ^ (mask | 0x8000000000000000ULL);
}


