#pragma once

template <typename T = void, 
          bool SimdHash = true>
void one_hot_encoding(unsigned int x)
{
    one_hot_encoding_mt<T,
                        1, //CORES
                        SimdHash>(x);
}


