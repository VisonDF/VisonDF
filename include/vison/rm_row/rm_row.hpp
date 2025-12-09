#pragma once

template <bool MemClean = false,
          bool Soft = false>
void rm_row(unsigned int x) 
{

    rm_row_mt<1, 
              MemClean,
              Soft>(x);

};




