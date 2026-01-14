#pragma once

template <
          bool IsDense   = false, // assumed sorted
          bool IsSorted  = true, 
          bool IdxIsTrue = true,
          AssertionType AssertionLevel = AssertionType::Normal
         >
void get_dataframe_filter_idx(
                              const std::vector<size_t>& cols, 
                              Dataframe& cur_obj,
                              const std::vector<unsigned int>& mask,
                              Runs& runs = default_idx_runs
                             )
{

    get_dataframe_filter_idx_mt<1,     // CORES
                                false, // NUMA locality
                                IsDense,
                                IsSorted,
                                IdxIsTrue,
                                AssertionLevel>(cols, 
                                                cur_obj, 
                                                mask, 
                                                runs);

}



