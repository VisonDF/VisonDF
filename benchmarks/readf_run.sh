g++ -Ofast -std=c++20 -march=native -mtune=native -fopenmp -fno-rtti ../include/vison/vison.hpp
g++ -Ofast -std=c++20 -march=native -mtune=native -fopenmp -fno-rtti readf.cpp -o readf_bench
./readf_bench
