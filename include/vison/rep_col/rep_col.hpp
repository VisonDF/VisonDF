#pragma once

template <bool IsBool                  = false,
          bool MapCol                  = false,
          AssertionType AssertionLevel = AssertionType::None,
          typename T>
requires span_or_vec<T>
void rep_col(
             const T &x, 
             const unsigned int colnb
            ) 
{

    rep_col_range_mt<1,     // CORES
                     false, // NUMA locality
                     IsBool,
                     MapCol,
                     AssertionLevel
                    >(x, 
                      colnb,
                      0,       // strt_vl
                      0,       // start of x
                      x.size() // end of x
                      );

};




