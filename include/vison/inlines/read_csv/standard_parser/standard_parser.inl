
template <unsigned int strt_row, unsigned int end_row>
inline void standard_parser(std::string_view& csv_view,
                            const char delim,
                            const char str_context,
                            const size_t ncol,
                            const size_t i,
                            const std::vector<size_t>& newline_pos,
                            const bool header_name
                            ) 
{

    if constexpr (strt_row == 0 && end_row == 0) {

        std::string_view csv_view2(csv_view.data() + i, csv_view.size() - i);

        parse_rows_chunk(csv_view2,
                         tmp_val_refv2,
                         delim,
                         str_context,
                         ncol);
      
      } else if constexpr (strt_row != 0 && end_row != 0) {
  
          nrow = end_row - strt_row;  
          std::string_view csv_view2(csv_view.data() + newline_pos[strt_row], 
                                     newline_pos[strt_row] - newline_pos[end_row] + 1);

          parse_rows_chunk(csv_view2,
                           tmp_val_refv2,
                           delim,
                           str_context,
                           ncol);


      } else if constexpr (strt_row != 0) {
  
          nrow -= strt_row;  
          std::string_view csv_view2(csv_view.data() + newline_pos[strt_row], 
                                     csv_view.size() - newline_pos[strt_row]);

          parse_rows_chunk(csv_view2,
                           tmp_val_refv2,
                           delim,
                           str_context,
                           ncol);
 
      } else if constexpr (end_row != 0) {
  
          nrow = end_row;
          std::string_view csv_view2(csv_view.data(), newline_pos[end_row] + 1);

          parse_rows_chunk(csv_view2,
                           tmp_val_refv2,
                           delim,
                           str_context,
                           ncol);

      }
}



