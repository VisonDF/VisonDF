#include "../vison/vison.hpp"   // your header defining Dataframe
#include <random>

int main(int argc, char** argv) {

    std::string file_name = "csvs/Airline_Delay_Cause.csv";

    //std::string file_name = "csv_test/outb3.csv";

    int runs = 5;

    Dataframe obj1;
    double total_time = 0.0;

    std::cout << "Benchmarking parser on file: " << file_name << "\n";
    std::cout << "Delimiter: ','  |  Quote: '\"'  |  Header: yes\n";
    std::cout << "----------------------------------------------------\n";

    for (int i = 1; i <= runs; ++i) {
        auto t0 = std::chrono::high_resolution_clock::now();
        obj1.readf<0, 0, 4, 0, 0>(file_name, ',', true, '"');
        auto t1 = std::chrono::high_resolution_clock::now();

        double elapsed = std::chrono::duration<double>(t1 - t0).count();
        total_time += elapsed;
 
        std::cout << "Run " << i << ": " << elapsed << " s"
                  << " | nrows=" << obj1.get_nrow()
                  << " | ncols=" << obj1.get_ncol() << "\n";

        // Optional: reset between runs to avoid accumulation
        if (i < runs)
          obj1.reinitiate();  
    }

    double avg = total_time / runs;
    std::cout << "----------------------------------------------------\n";
    std::cout << "Average over " << runs << " runs: " << avg << " s\n";
    std::cout << "Rows: " << obj1.get_nrow() << "  |  Cols: " << obj1.get_ncol() << "\n";

    // Optional: throughput in MB/s
    std::ifstream file(file_name, std::ios::binary | std::ios::ate);
    size_t file_size = file.tellg();
    std::cout << "Throughput: " << (file_size / avg / (1024.0 * 1024.0))
              << " MB/s\n";

    std::cout << "Head:\n";

    std::vector<int> rvec = {};

    std::vector<int> cvec = {};

    std::random_device rd;

    std::mt19937 gen(rd());

    int a = 0;
    int b = obj1.get_nrow() - 1;
    std::uniform_int_distribution<> dist(a, b);

    for (int i = 0; i < 10; ++i) {
        int r = dist(gen);
        rvec.push_back(r);
    }

    a = 0;
    b = obj1.get_ncol() - 1;
    std::uniform_int_distribution<> dist2(a, b);

    for (int i = 0; i < 2; ++i) {
        int r = dist2(gen);
        cvec.push_back(r);
    }

    cvec.push_back(5);

    rvec.push_back(2048058);

    obj1.display_filter_idx(rvec, cvec);

    std::cout << "type_col: ";

    const std::vector<char>& vectype = obj1.get_typecol();
    for (auto& el : vectype) {
      std::cout << el << " ";
    }

    std::cout << "\n";

    return 0;
}




