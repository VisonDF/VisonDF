#pragma once

struct RunsIdxMt {
    size_t mask_pos;
    size_t src_start;
    size_t len;
};

struct Runs {
    std::vector<RunsIdxMt> vec;
    std::vector<size_t> thread_offsets;
}
