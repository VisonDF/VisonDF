#pragma once

void display(const std::vector<unsigned int>& cols)
{
    const unsigned int local_nrow = nrow;
    display_range_mt<1>(cols,
                        0, //strt
                        local_nrow);
}

