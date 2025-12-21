#pragma once

void unmaterialize()
{
    if (in_view) {
        std::cerr << "Can't perform this operation, already in `view` mode, consider applying `.materailize()`\n";
        return;
    }
    const unsigned int local_nrow = nrow;
    row_view_idx.resize(local_nrow);
    std::iota(row_view_idx.begin(), row_view_idx.end(), 0);
    for (size_t i = 0; i < local_nrow; ++i)
        row_view_map.emplace(i, i);
    in_view = false;
}





