#include<iostream>
#include<fstream>
#include<algorithm>
#include<iterator>

int main(int argc, char** argv) {
    std::string fileName = argv[1];
    std::ifstream myfile(fileName);

    // new lines will be skipped unless we stop it from happening:    
    myfile.unsetf(std::ios_base::skipws);

    // count the newlines with an algorithm specialized for counting:
    unsigned line_count = std::count(
        std::istream_iterator<char>(myfile),
        std::istream_iterator<char>(), 
        '\n');

    std::cout << "Lines: " << line_count << "\n";
    return 0;
}
