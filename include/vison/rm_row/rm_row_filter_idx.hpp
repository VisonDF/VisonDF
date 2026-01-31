#pragma once

template <MtMethod MtType              = MtMethod::Row,
          bool MemClean                = false,
          bool Soft                    = true,
          bool IdxIsTrue               = true,
          AssertionType AssertionLevel = AssertionType::Simple,
          typename T
         >
requires span_or_vec<T>
void rm_row_filter_idx(
                       const T& mask,
                       Runs& runs,
                       const unsigned int periodic_mask_len
                      ) 
{

    rm_row_filter_idx_mt<1,     // CORES
                          false, // NUMA locality
                          MType,
                          MemClean,
                          Soft,
                          IdxIsTrue,
                          AssertionLevel>(
        mask, 
        runs, 
        periodic_mask_len
    );

}

template <MtMethod MtType              = MtMethod::Row,
          bool MemClean                = false,
          bool Soft                    = true,
          bool IdxIsTrue               = true,
          AssertionType AssertionLevel = AssertionType::Simple,
          typename T
         >
requires span_or_vec<T>
void rm_row_filter_idx(
                       const T& mask,
                       Runs& runs = default_idx_runs
                      ) 
{

    rm_row_filter_idx_mt<1,      // CORES
                          false, // NUMA locality
                          MType,
                          MemClean,
                          Soft,
                          IdxIsTrue,
                          AssertionLevel>(
        mask, 
        runs,
        nrow
    );

}




