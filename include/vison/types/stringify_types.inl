#pragma once

template <typename T>
constexpr std::string_view type_name() {

    std::string_view p = __PRETTY_FUNCTION__;
    auto start = p.find("with T = ") + 9;
    auto end   = p.find(';', start);
    return p.substr(start, end - start);

}



