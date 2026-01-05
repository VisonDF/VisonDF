#pragma once

template <bool IsBool = false,
          bool MapCol = false,
          typename T>
void get_col_range(const unsigned int x, 
                   std::vector<T> &rtn_v,
                   const unsigned int strt,
                   const unsigned int end)
{

    get_col_range_mt<1, IsBool, MapCol>(x,
                                        rtn_v,
                                        strt,
                                        end);

}

