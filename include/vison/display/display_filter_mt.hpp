#pragma once

template <unsigned int CORES = 4>
void display_filter_mt(const std::vector<uint8_t>& mask,
                       std::vector<unsigned int>& cols) 
{
   
   display_filter_range_mt<CORES>(mask, 
                                  0, // strt
                                  cols);

};



