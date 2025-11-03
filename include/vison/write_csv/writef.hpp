#pragma once

void writef(std::string &file_name, char delim = ',', bool header_name = 1, char str_context_bgn = '\'', char str_context_end = '\'') {
  unsigned int i2;
  unsigned int i3;
  std::fstream outfile(file_name, std::ios::out);
  std::string cur_str;
  if (header_name) {
    i2 = 0;
    while (i2 + 1 < ncol) {
      outfile << name_v[i2];
      outfile << delim;
      i2 += 1;
    };
    outfile << name_v[i2];
    outfile << "\n";
  };
  for (unsigned int i = 0; i < nrow; ++i) {
    i2 = 0;
    while (i2 + 1 < ncol) {
      cur_str = tmp_val_refv[i2][i];
      if (type_refv[i2] == 's') {
        for (i3 = 0; i3 < cur_str.length(); ++i3) {
          if (cur_str[i3] == delim) {
            cur_str.insert(0, 1, str_context_bgn);
            cur_str.push_back(str_context_end);
            break;
          };
        };
      } else if (cur_str[0] == delim) {
        cur_str.insert(0, 1, str_context_bgn);
        cur_str.push_back(str_context_end);
      };
      outfile << cur_str;
      i2 += 1;
      outfile << delim;
    };
    cur_str = tmp_val_refv[i2][i];
    if (type_refv[i2] == 's') {
      for (i3 = 0; i3 < cur_str.length(); ++i3) {
        if (cur_str[i3] == delim) {
          cur_str.insert(0, 1, str_context_bgn);
          cur_str.push_back(str_context_end);
          break;
        };
      };
    } else if (cur_str[0] == delim) {
      cur_str.insert(0, 1, str_context_bgn);
      cur_str.push_back(str_context_end);
    };
    outfile << cur_str;
    outfile << "\n";
  };
};


