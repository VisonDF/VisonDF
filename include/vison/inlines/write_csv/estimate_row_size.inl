#pragma once

inline size_t estimate_row_size(size_t sample_rows = 50) 
{
    
    sample_rows = std::min(sample_rows, static_cast<size_t>(nrow));

    const size_t delim_and_newline_bytes = 1;     
    const size_t quote_bytes = 2; 

    size_t total = 0;

    for (size_t i = 0; i < sample_rows; ++i) {
        size_t row_bytes = 0;

        for (size_t j = 0; j < ncol; ++j) {
            const std::string& cell = tmp_val_refv[j][i];

            row_bytes += cell.size();

            if (type_refv[j] == 's') {
                row_bytes += quote_bytes;
            }

            row_bytes += delim_and_newline_bytes;
        }

        total += row_bytes;
    }

    return total / sample_rows;
}


