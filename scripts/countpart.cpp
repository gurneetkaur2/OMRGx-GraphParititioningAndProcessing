#include<iostream>
#include<fstream>
#include<algorithm>
#include<iterator>

int main(int argc, char** argv) {
    std::string fileName = argv[1];
    int num = atoi(argv[2]);
    if(argc != 3){
      std::cout<<"Usage <partsfile> <nparts> will give you count in each part" <<std::endl;
      return 0;
      }

    std::ifstream myfile(fileName);

    // new lines will be skipped unless we stop it from happening:    
//    myfile.unsetf(std::ios_base::skipws);

    // count the newlines with an algorithm specialized for counting:
     std::cout <<"*************************************************\n \n";
     std::cout << "\nFileName: " <<fileName;
     unsigned count = 0, line_count = 0;
     std::vector<int> arr(num,0);

     int p;
     while(myfile >> p){
        int ind = p % num;
//     std::cout <<"\nIndex " << ind;
	arr[ind]++;
/*	if(p == num){
	   count++;
	   }*/
	   line_count++;
    }
//    unsigned line_count = std::count(
  //      std::istream_iterator<char>(myfile),
    //    std::istream_iterator<char>(), num);
  //  std::ofstream ofile;
  //  std::string outfile = "parts" + num;
   //   ofile.open (outfile.csv);
   for(int i=0; i<arr.size(); i++){
    	std::cout << "\nNUM : " << i  << " occurs " << arr[i] << " times \n";
//	ofile <<arr[i] <<"\n";
	}
//	ofile.close();
	std::cout<<"\nTotal lines " << line_count;
    return 0;
}
