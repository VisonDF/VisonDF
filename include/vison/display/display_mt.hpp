#pragma once

template <unsigned int CORES = 4>
void display_mt(std::vector<unsigned int>& cols) 
{
   
    const unsigned int local_nrow = nrow;
    display_range_mt<CORES>(cols,
                            0, //strt
                            local_nrow);

};



