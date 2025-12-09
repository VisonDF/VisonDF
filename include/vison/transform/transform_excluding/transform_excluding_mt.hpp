#pragma once

template <typename T = void,
          unsigned int CORES = 4, 
          bool MemClean = false, 
          bool SimdHash = true,
          bool Soft = true>
void transform_excluding_mt(Dataframe &cur_obj, 
                unsigned int in_col, 
                unsigned int ext_col) 
{

    transform_inner_excluding<T,
                              CORES, 
                              MemClean, 
                              SimdHash,
                              Soft,
                              false>(cur_obj, in_col, ext_col);
    
};


