#pragma once

template <typename T = void,
          bool MemClean = false, 
          bool SimdHash = true,
          bool Soft = true>
void transform_excluding(Dataframe &cur_obj, 
                unsigned int in_col, 
                unsigned int ext_col) 
{

    transform_inner_excluding<T,
                              1, //CORES 
                              MemClean, 
                              SimdHash,
                              Soft,
                              false //Inner
                             >(cur_obj, in_col, ext_col);

};


