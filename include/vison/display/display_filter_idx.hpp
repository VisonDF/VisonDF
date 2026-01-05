#pragma once

void display_filter_idx(const std::vector<size_t>& mask,
                        const std::vector<unsigned int>& cols)
{
    display_filter_idx_mt<1>(mask, cols);
}
