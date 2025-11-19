#pragma once

template <bool SimdHash = true>
void pivot(Dataframe &obj, 
           unsigned int &n1, 
           unsigned int& n2, 
           unsigned int& n3)
{

  switch (type_refv[n3]) {

          case 'i': pivot_int<SimdHash>(obj, n1, n2, n3); break;
          case 'u': pivot_uint<SimdHash>(obj, n1, n2, n3); break;
          case 'd': pivot_dbl<SimdHash>(obj, n1, n2, n3); break;
          default: {

                           std::cerr << "Pivot supported types are: IntT, UIntT and FloaT\n";
                           return;

                   }

  }

}
