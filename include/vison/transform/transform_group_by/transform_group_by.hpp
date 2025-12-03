#pragma once

template <typename T,
          bool Occurence = false,
          bool SimdHash = true>
void transform_group_by(const std::vector<unsigned int>& x,
                        const n_col int = -1,
                        const std::string colname = "n") 
{

    transform_group_by_mt<T,
                          1,
                          Occurence,
                          SimdHash>(x,
                                    n_col,
                                    colname);

}


