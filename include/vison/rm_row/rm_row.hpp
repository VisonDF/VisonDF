#pragma once

template <bool MemClean = false>
void rm_row(unsigned int x) 
{

    rm_row_mt<1, 
              MemClean>(x);

};




