g++ -O3 -std=c++20 -fopenmp -march=native -funroll-loops -ftree-vectorize -fvect-cost-model=dynamic -fno-rtti ../vison/vison.hpp
g++ -O3 -std=c++20 -fopenmp -march=native -funroll-loops -ftree-vectorize -fvect-cost-model=dynamic -fno-rtti readf.cpp -o readf_bench
./readf_bench
