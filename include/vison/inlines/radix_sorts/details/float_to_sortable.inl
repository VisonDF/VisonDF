#pragma once

uint32_t float_to_sortable(float f) {
    uint32_t bits;
    memcpy(&bits, &f, sizeof(bits));   

    uint32_t mask = -(bits >> 31);     // 0xFFFFFFFF if negative, else 0
    return bits ^ (mask | 0x80000000u);
}
