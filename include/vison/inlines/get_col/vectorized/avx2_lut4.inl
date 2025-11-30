#pragma once

struct Mask4LUT {
    uint8_t idx[4];
    uint8_t count;
};

static constexpr Mask4LUT LUT4[16] = {
    {{0,0,0,0}, 0},       // 0000 -> keep none
    {{0,0,0,0}, 1},       // 0001 -> keep 0
    {{1,0,0,0}, 1},       // 0010 -> keep 1
    {{0,1,0,0}, 2},       // 0011 -> keep 0,1
    {{2,0,0,0}, 1},       // 0100 -> keep 2
    {{0,2,0,0}, 2},       // 0101 -> keep 0,2
    {{1,2,0,0}, 2},       // 0110 -> keep 1,2
    {{0,1,2,0}, 3},       // 0111 -> keep 0,1,2
    {{3,0,0,0}, 1},       // 1000 -> keep 3
    {{0,3,0,0}, 2},       // 1001 -> keep 0,3
    {{1,3,0,0}, 2},       // 1010 -> keep 1,3
    {{0,1,3,0}, 3},       // 1011 -> keep 0,1,3
    {{2,3,0,0}, 2},       // 1100 -> keep 2,3
    {{0,2,3,0}, 3},       // 1101 -> keep 0,2,3
    {{1,2,3,0}, 3},       // 1110 -> keep 1,2,3
    {{0,1,2,3}, 4},       // 1111 -> keep all
};

// Compress 4x64-bit chunk according to 4-bit mask, scalar but branch-light.
template <typename T>
inline int compress4_lut(const T* src, 
                            uint8_t mask4, 
                            T* dst) {
    const Mask4LUT &e = LUT4[mask4];
    int n = e.count;
    if (n >= 1) dst[0] = src[e.idx[0]];
    if (n >= 2) dst[1] = src[e.idx[1]];
    if (n >= 3) dst[2] = src[e.idx[2]];
    if (n >= 4) dst[3] = src[e.idx[3]];
    return n;
}


