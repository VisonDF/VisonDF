#pragma once

template <typename T = void,
          unsigned int CORES = 4, 
          bool MemClean = false, 
          bool SimdHash = true>
void transform_inner_mt(Dataframe &cur_obj, 
                        unsigned int in_col, 
                        unsigned int ext_col) 
{

    transform_inner_excluding<T,
                              CORES, 
                              MemClean, 
                              SimdHash,
                              true>(cur_obj, in_col, ext_col);

};


