#pragma once

void display_filter(const std::vector<uint8_t>& mask,
                    std::vector<unsigned int>& cols
                   )
{
    dislay_filter_range_mt<1>(mask, 
                              0, // strt
                              cols);
}

