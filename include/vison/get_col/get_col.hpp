#pragma once

template <bool IsBool = false,
          bool MapCol = false,
          typename T>
void get_col()
{

    const unsigned int local_nrow = nrow;
    get_col_range_mt<1, // CORES
                     false, // NUMA locality
                     IsBool, 
                     MapCol>(x, 
                             rtn_v,
                             0, // strt
                             local_nrow);

}

