#pragma once

template <bool IsBool                  = false,
          bool MapCol                  = false,
          AssertionType AssertionLevel = AssertionType::None,
          typename T> 
void rep_col_range(
                   std::vector<T> &x, 
                   const unsigned int colnb,
                   const unsigned int strt_vl
                  ) 
{

    rep_col_range_mt<1,     // CORES
                     false, // NUMA locality
                     IsBool,
                     MapCol,
                     AssertionLevel
                    >(x, 
                      colnb,
                      strt_vl
                     );

};




