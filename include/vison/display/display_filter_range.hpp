#pragma once

void display_filter_range(const std::vector<uint8_t>& mask,
                          const size_t strt_vl,
                          const std::vector<unsigned int>& cols)
{

    dislay_filter_range_mt<1>(mask, 
                              strt_vl,
                              cols);
}
