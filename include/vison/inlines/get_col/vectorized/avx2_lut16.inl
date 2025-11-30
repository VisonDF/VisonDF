#pragma once

struct Lut16Entry {
    uint8_t count;       
    uint8_t shuf[16];    
};

constexpr Lut16Entry make_entry(uint16_t m)
{
    Lut16Entry e{};
    uint8_t n = 0;

    for (uint8_t bit = 0; bit < 16; ++bit) {
        if (m & (1u << bit))
            e.shuf[n++] = bit;
    }

    e.count = n;

    for (uint8_t i = n; i < 16; ++i)
        e.shuf[i] = 0x80;

    return e;
}

constexpr auto make_LUT16()
{
    std::array<Lut16Entry, 1u << 16> lut{};

    for (uint16_t m = 0; m < (1u << 16); ++m)
        lut[m] = make_entry(m);

    return lut;
}

constexpr auto LUT16 = make_LUT16();


