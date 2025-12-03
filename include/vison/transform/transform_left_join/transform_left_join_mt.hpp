#pragma once

template <typename T,
          unsigned int CORES = 4,
          bool SimdHash = true, 
          LeftJoinMethods Method = LeftJoinMethods::First>
void transform_left_join_mt(Dataframe &obj, 
                            const unsigned int &key1, 
                            const unsigned int &key2,
                            const CharT, default_chr,
                            const std::string default_str = "NA",
                            const uint8_t default_bool = 0,
                            const IntT default_int = 0,
                            const UIntT default_uint = 0,
                            const FloatT default_dbl = 0) 
{

    transform_left_join<T, 
                        SimdHash, 
                        Method, 
                        CORES>(obj,
                               key1,
                               key2,
                               default_chr,
                               default_str,
                               default_bool,
                               default_int,
                               default_uint,
                               default_dbl);

};




