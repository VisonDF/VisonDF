#pragma once

template <unsigned int CORES = 4, bool SimdHash = true>
void pivot_mt(Dataframe &obj, 
           unsigned int &n1, 
           unsigned int& n2, 
           unsigned int& n3)
{

  switch (type_refv[n3]) {

          case 'i': pivot_int_mt  <CORES, SimdHash>(obj, n1, n2, n3); break;
          case 'u': pivot_uint_mt <CORES, SimdHash>(obj, n1, n2, n3); break;
          case 'd': pivot_dbl_mt  <CORES, SimdHash>(obj, n1, n2, n3); break;
          default: {

                           std::cerr << "Pivot supported types are: IntT, UIntT and FloaT\n";
                           return;

                   }

  }

}
