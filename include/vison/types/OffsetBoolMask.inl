#pragma once

struct OffsetBoolMask {
    std::vector<unsigned int> thread_offsets;
    unsigned int active_rows;
}

inline OffsetBoolMask default_offset_start{{}, 0};
