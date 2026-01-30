#pragma once

template <bool SimdHash = true>
void otm(Dataframe &obj_l,
         Dataframe &obj_r,
         const unsigned int &key1, 
         const unsigned int &key2,
         const CharT default_chr,
         const std::string default_str = "NA",
         const uint8_t default_bool = 0,
         const IntT default_int = 0,
         const UIntT default_uint = 0,
         const FloatT default_decimal = 0) 
{

    otm_mt<1,     // CORES
           false, // NUMA locality
           false, // Nested
           SimdHash>(obj_l,
                     obj_r,
                     key1,
                     key2,
                     default_chr,
                     default_str,
                     default_bool,
                     default_int,
                     default_uint,
                     default_decimal);

};




