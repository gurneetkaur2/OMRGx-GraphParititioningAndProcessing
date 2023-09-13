#include <iostream>
#include <fstream>
#include <sstream>
#include <set>
#include <cassert>
#include <cstdlib>
#include <sys/stat.h>
#include <sys/types.h>

//#include "metis.h"

#define BASE_PATH "../inputs/partsD"
//#define PARTS_PATH "parts_"

#define SOURCE 0
// Count the number of vertices in each partition from mtmetis generated partitions

using namespace std;
typedef int64_t idx_t;
//typedef int32_t idx_t;
//typedef float real_t;

typedef struct edgeList
{
  set<unsigned long long> edges;
} EdgeList;

int main(int argc, char* argv[])
{
  assert(argc == 2);
  string fileName = argv[1];

   *idx_t npart = atoi(argv[2]); //which part to extract to get its complete partition size

  /*idx_t nparts = atoi(argv[2]);

  string partsDir = "";
  partsDir += PARTS_PATH;
  partsDir += argv[2];
  partsDir += "/";

  mkdir((BASE_PATH + partsDir).c_str(), 0777); 
*/
  unsigned long long num_edges = 0, num_vertices = 0;
  {
  string partPath = BASE_PATH + fileName + npart;
  ofstream partFile;
  partFile.open(partPath.c_str());
  assert(partFile.is_open());

  unsigned long long to;
  cout <<"\nPartPath: " << partPath  << "\tfileName " << fileName;

  std::ifstream infile(fileName.c_str());
  //infile >> to >> from;
    while (infile >> to ) {
	if (to == npart){
	   partFile << to << std::endl;
	}
    }
  }
  partFile.close();

}

