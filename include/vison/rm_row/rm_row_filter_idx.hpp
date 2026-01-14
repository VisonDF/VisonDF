#pragma once

template <MtMethod MtType              = MtMethod::Row,
          bool MemClean                = false,
          bool Soft                    = true,
          bool Sorted                  = true,
          bool IdxIsTrue               = true,
          AssertionType AssertionLevel = AssertionType::Simple
         >
void rm_row_filter_idx(
                       std::vector<uint8_t>& mask,
                       Runs& runs = default_idx_runs
                      ) 
{

    rm_row_filter_idx_mt<1,     // CORES
                          false, // NUMA locality
                          MType,
                          MemClean,
                          Soft,
                          Sorted,
                          IdxIsTrue,
                          AssertionLevel>(mask, runs);

}
