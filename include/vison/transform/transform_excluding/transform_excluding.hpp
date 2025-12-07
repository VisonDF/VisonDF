#pragma once

template <bool MemClean = false, bool SimdHash = true>
void transform_excluding(Dataframe &cur_obj, 
                unsigned int in_col, 
                unsigned int ext_col) 
{

    transform_excluding_mt<1, MemClean, SimdHash>(cur_obj,
                                                  in_col,
                                                  ext_col);

};


