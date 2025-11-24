#pragma once

template <unsigned int CORES = 4>
void display_filter_range(const std::vector<uint8_t>& mask,
                          const size_t strt,
                          std::vector<unsigned int>& cols) {
   
    if (cols.empty()) {
        cols.resize(ncol);
        for (size_t i = 0; i < ncol; ++i)
            cols[i] = i;
    }

    longest_determine<CORES>();
    unsigned int max_nblngth;

    if (name_v_row.size() == 0) {
        max_nblngth = std::to_string(nrow).length();
    } else {
        for (size_t i = 0; i < mask.size(); ++i) {
            if (!mask[i]) continue;
            if (name_v_row[strt + i].size() > max_nblngth)
                max_nblngth = name_v_row[strt + i].size();
        };
    };

    std::cout << std::string(max_nblngth + 2, ' ');

    for (size_t& i : cols) {

        switch(type_refv[i]) {

            case 's': {
                          std::cout << type_print_vec[0];
                          longest_v[i] = (longest_v[i] < ref_type_size[0]) ? ref_type_size[0] : longest_v[i];
                          break;
                      }
            case 'c': {
                          std::cout << type_print_vec[1];
                          longest_v[i] = (longest_v[i] < ref_type_size[1]) ? ref_type_size[1] : longest_v[i];
                          break;
                      }
            case 'b': {
                          std::cout << type_print_vec[2];
                          longest_v[i] = (longest_v[i] < ref_type_size[2]) ? ref_type_size[2] : longest_v[i];
                          break;
                      }
            case 'i': {
                          std::cout << type_print_vec[3];
                          longest_v[i] = (longest_v[i] < ref_type_size[3]) ? ref_type_size[3] : longest_v[i];
                          break;
                      }
            case 'u': {
                          std::cout << type_print_vec[4];
                          longest_v[i] = (longest_v[i] < ref_type_size[4]) ? ref_type_size[4] : longest_v[i];
                          break;
                      }
            case 'd': {
                          std::cout << type_print_vec[5];
                          longest_v[i] = (longest_v[i] < ref_type_size[5]) ? ref_type_size[5] : longest_v[i];
                          break;
                      }

        }

        std::cout << std::string(longest_v[i] - longest_v[i], ' ');

    };
    
    std::cout << "\n";
    std::cout << std::string(max_nblngth + 2, ' ');
   
    if (name_v.size() > 0) {
        for (size_t& i : cols) {
            std::cout << name_v[i] << " ";
            std::cout << std::string(longest_v[i] - name_v[i].length(), ' ');
        };
    } else {
        for (size_t& i : cols) {
            const std::string cur_str = "[" + std::to_string(i) + "]";
            std::cout << cur_str << " ";
            std::cout << std::string(longest_v[i] - cur_str.length(), ' ');
        };
    };
    
    std::cout << "\n";
    
    if (name_v_row.size() == 0) {
        for (size_t i = 0; i < mask.size(); ++i) {

            if (!mask[i]) continue;

            std::cout << ":" << (strt + i) << ": ";
            std::cout << std::string(max_nblngth - std::to_string(strt + i).length(), ' ');

            for (size_t& i2 : cols) {
                const std::string& cur_str = tmp_val_refv[i2][strt + i];
                std::cout << cur_str << " ";
                std::cout << std::string(longest_v[i2] - cur_str.length(), ' ');
            };

            std::cout << "\n";

        };
    } else {
        for (size_t i = 0; i < mask.size(); ++i) {

            if (!mask[i]) continue;

            std::cout << name_v_row[strt + i] << " : ";
            std::cout << std::string(max_nblngth - std::to_string(strt + i).length(), ' ');

            for (size_t& i2 : cols) {
                const std::string& cur_str = tmp_val_refv[i2][strt + i];
                std::cout << cur_str << " ";
                std::cout << std::string(longest_v[i2] - cur_str.length(), ' ');
            };

            std::cout << "\n";

        };

    };

};



