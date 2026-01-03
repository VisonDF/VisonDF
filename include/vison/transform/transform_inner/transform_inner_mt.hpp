#pragma once

template <typename T = void,
          unsigned int CORES = 4, 
          bool MemClean = false, 
          bool SimdHash = true,
          bool Soft = true,
          bool MapCol = false
         >
void transform_inner_mt(Dataframe &cur_obj, 
                        unsigned int in_col, 
                        unsigned int ext_col) 
{

    if (in_view && !Soft) {
        std::cerr << "Can't perform this operation while in `view` mode, consider applying `.materialize()`\n"
        return;
    }

    transform_inner_excluding<T,
                              CORES, 
                              MemClean, 
                              SimdHash,
                              Soft,
                              true, //Inner
                              MapCol
                              >(cur_obj, in_col, ext_col);

};


