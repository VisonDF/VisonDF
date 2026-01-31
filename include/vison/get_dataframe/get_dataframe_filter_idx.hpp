#pragma once

template <
          bool IsDense                 = false, // assumed sorted
          bool MapCol                  = false,
          bool IdxIsTrue               = true,
          bool Periodic                = false,
          AssertionType AssertionLevel = AssertionType::Normal,
          typename T
         >
requires span_or_vec<T>
void get_dataframe_filter_idx(
                              const std::vector<size_t>& cols, 
                              Dataframe& cur_obj,
                              const T& mask,
                              Runs& runs,
                              const unsigned int periodic_mask_len
                             )
{

    get_dataframe_filter_idx_mt<1,     // CORES
                                false, // NUMA locality
                                IsDense,
                                MapCol,
                                IdxIsTrue,
                                Periodic,
                                AssertionLevel>(
        cols, 
        cur_obj, 
        mask, 
        runs,
        periodic_mask_len
    );

}

template <
          bool IsDense                 = false, // assumed sorted
          bool MapCol                  = false,
          bool IdxIsTrue               = true,
          bool Periodic                = false,
          AssertionType AssertionLevel = AssertionType::Normal,
          typename T
         >
requires span_or_vec<T>
void get_dataframe_filter_idx(
                              const std::vector<size_t>& cols, 
                              Dataframe& cur_obj,
                              const T& mask,
                              Runs& runs = default_idx_runs
                             )
{

    get_dataframe_filter_idx_mt<1,     // CORES
                                false, // NUMA locality
                                IsDense,
                                MapCol,
                                IdxIsTrue,
                                Periodic,
                                AssertionLevel>(
        cols, 
        cur_obj, 
        mask, 
        runs,
        cur_obj.get_nrow()
    );

}









