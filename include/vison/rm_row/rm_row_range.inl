#pragma once

template <MtMethod MtType              = MtMethod::Row,
          bool MemClean                = false,
          bool Soft                    = true,
          bool Sorted                  = true,
          bool IdxIsTrue               = true,
          AssertionType AssertionLevel = AssertionType::Simple
         >
void rm_row_range(std::vector<uint8_t>& mask,
                  Runs& runs = Runs{}) 
{

    rm_row_range_mt<1,     // CORES
                    false, // NUMA locality
                    MType,
                    MemClean,
                    Soft,
                    Sorted,
                    IdxIsTrue,
                    AssertionLevel>(mask, runs);

}
