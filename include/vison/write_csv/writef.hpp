#pragma once

void writef(const std::string& file_name,
                    size_t target_chunk_bytes = 100'000'000,
                    const char delim = ',',
                    const bool header_name = true,
                    const char str_context = '\'') {

    if (nrow == 0) return;

    std::ofstream outfile(file_name, std::ios::binary);
    if (!outfile) throw std::runtime_error("Cannot open file");

    if (header_name && ncol > 0) {
        for (unsigned int j = 0; j < ncol; ++j) {
            if (j) outfile.put(delim);
            outfile.write(name_v[j].data(), name_v[j].size());
        }
        outfile.put('\n');
    }

    const size_t bytes_per_row = estimate_row_size(50);

    size_t rows_per_chunk = std::max<size_t>(1, target_chunk_bytes / bytes_per_row);

    std::string buffer;
    buffer.reserve(std::min(target_chunk_bytes + 4096, static_cast<size_t>(256'000'000)));

    size_t rows_in_buff = 0;

    auto flush = [&]() {
        if (!buffer.empty()) {
            outfile.write(buffer.data(), buffer.size());
            buffer.clear();
        }
    };

    for (size_t i = 0; i < static_cast<size_t>(nrow); ++i) {
        
        for (unsigned int j = 0; j < ncol; ++j) {
            const std::string& cell = tmp_val_refv[j][i];

            if (type_refv[j] == 's') {
                buffer.push_back(str_context);
                buffer.append(cell);
                buffer.push_back(str_context);
            } else {
                buffer.append(cell);
            }

            if (j + 1 < ncol) buffer.push_back(delim);
        }

        buffer.push_back('\n');
        rows_in_buff += 1;

        if (rows_in_buff >= rows_per_chunk) {
            flush();
            rows_in_buff = 0;
        }
    }

    flush();
}




