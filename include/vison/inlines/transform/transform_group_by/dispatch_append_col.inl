#pragma once

template <bool RsltType>
inline void dispatch_append_col(auto& v_col,
                                const size_t idx_type)
{
    if constexpr (RsltType) {

        switch (idx_type) {
            case 0: type_refv.push_back('s'); str_v.push_back (v_col);  break;
            case 1: type_refv.push_back('c'); chr_v.push_back (v_col);  break;
            case 2: type_refv.push_back('b'); bool_v.push_back(v_col);  break;
            case 3: type_refv.push_back('i'); int_v.push_back (v_col);  break;
            case 4: type_refv.push_back('u'); uint_v.push_back(v_col);  break;
            case 5: type_refv.push_back('d'); dbl_v.push_back( v_col);  break;
        }

    } else {
        std::visit([](auto&& ptr) {

            using TP = std:decay_t<decltype(ptr)>;

            if constexpr (!std::is_same_v<TP, std::monostate>) {
                using Elem = TP::value_type;

                if constexpr (std::is_same_v<Elem, std::string>) {

                    type_refv.push_back('s'); 
                    str_v.push_back (v_col);

                } else if constexpr (std::is_same_v<Elem, CharT>) {

                    type_refv.push_back('c'); 
                    chr_v.push_back (v_col);

                } else if constexpr (std::is_same_v<Elem, uint8_t>) {

                    type_refv.push_back('b'); 
                    bool_v.push_back (v_col);

                } else if constexpr (std::is_same_v<Elem, IntT>) {

                    type_refv.push_back('i'); 
                    int_v.push_back (v_col);

                } else if constexpr (std::is_same_v<Elem, UIntT>) {

                    type_refv.push_back('u'); 
                    uint_v.push_back (v_col);

                } else {

                    type_refv.push_back('d'); 
                    dbl_v.push_back (v_col);

                }

            }

        }, v_col);
    }
}


