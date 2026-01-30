#pragma once

template <unsigned int CORES           = 4,
          bool NUMA                    = false,
          bool IsBool                  = false,
          bool MapCol                  = false,
          AssertionType AssertionLevel = AssertionType::None,
          typename T> 
void rep_col_mt(std::vector<T> &x, 
                const unsigned int colnb) 
{

    rep_col_range_mt<CORES,
                     NUMA,
                     IsBool,
                     MapCol,
                     AssertionLevel>(x, 
                                     colnb, 
                                     0 // strt_vl
                                     );

};




