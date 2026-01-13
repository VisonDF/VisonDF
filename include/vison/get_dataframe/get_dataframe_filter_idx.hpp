#pragma once

template <
          bool IsDense   = false, // assumed sorted
          bool IsSorted  = true, 
          bool IdxIsTrue = true,
         >
void get_dataframe_filter_idx(const std::vector<size_t>& cols, 
                              Dataframe& cur_obj,
                              const std::vector<unsigned int>& mask,
                              std::vector<RunsIdxMt>& runs = {}
                             )
{

    get_dataframe_filter_idx_mt<1,     // CORES
                                false, // NUMA locality
                                IsDense,
                                IsSorted,
                                IdxIsTrue>(cols, 
                                           cur_obj, 
                                           mask, 
                                           runs);

}



