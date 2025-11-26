#pragma once

struct MatchGroup {
    std::vector<size_t> idxs;
    size_t next = 0; 
};
