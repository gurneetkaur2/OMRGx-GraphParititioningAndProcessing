#include <iostream>
#include <fstream>
#include <sstream>
#include <set>
#include <cassert>
#include <cstdlib>
#include <sys/stat.h>
#include <sys/types.h>

//#include "./metis/include/metis.h"

#define BASE_PATH "../inputs/"
#define PARTS_PATH "parts_"

#define SOURCE 0

using namespace std;
typedef int64_t idx_t;

// Convert the graph and its partitions to a transformed graph

typedef struct edgeList
{
  set<unsigned long long> edges;
} EdgeList;

int main(int argc, char* argv[])
{
  if(argc !=5){
    cout << "\nUsage: <input adj_graph> <partsfilename> <nparts> <num_vertices> \n";
  assert(argc == 5);
  }
  string graphName = argv[1];
  string partFName = argv[2];
 // string adjGraphName = "adj_" + graphName;
  string boundariesName = graphName + ".boundaries";
//  string commName = graphName + ".comm";
  string sourceName = graphName + ".src";
 // string suffix = argv[5];
  idx_t nparts = atoi(argv[3]);
  unsigned long long  num_vertices = atoi(argv[4]);
  cout <<"\nfileName " << graphName;

 /* string partsDir = "";
  partsDir += PARTS_PATH;
  partsDir += argv[3];
  partsDir += "/";
 */  string partsDir;
   // for(int i = 8; i < 25; i = i+8){
           partsDir = "";
           partsDir += PARTS_PATH;
           partsDir += argv[3];
           partsDir += "/";

  mkdir((BASE_PATH + partsDir).c_str(), 0777); 

  cout <<"\nBase path " << (BASE_PATH + partsDir) ;
  // }
 // EdgeList* edgeLists = new EdgeList[num_vertices];
  ++num_vertices;
  cout <<"\nnum_vertices: " <<num_vertices;
  EdgeList* ogEdgeLists = new EdgeList[num_vertices];

  cout <<"\nGraph Path " << BASE_PATH + graphName ; 
  string graphPath = BASE_PATH + graphName;
  cout <<"\nBefore reading Adj file " ;

  std::ifstream infile(graphPath.c_str());
 // std::vector<unsigned> from;
  unsigned long long from;
  unsigned long long to = 0;
 // unsigned edge;
  std::string line;
//  ogEdgeLists[to].edges.insert(to); // for vertex 0
  while (std::getline(infile, line)){
          to++;
  //        cout <<"\n TO: " << to ;
         std::istringstream iss(line);
         while (iss >> from){
        ogEdgeLists[to].edges.insert(from);
 // cout << "\t from: " << from << endl;
          } 
      //cout <<"\n Read line TO " << to << "from " << from << endl;
  }
      cout <<"\n Read Graph " << endl;

  // new vertex IDs of vertices in the same partition
  unsigned long long* vertexToNewVertex = new unsigned long long[num_vertices];
  // stores the old vertex IDs 
  unsigned long long* newVertexToVertex = new unsigned long long[num_vertices];
 // unsigned long long new_id = 1;

  unsigned long long* part_boundaries = new unsigned long long[nparts];
  idx_t* part = new idx_t[num_vertices];
  //unsigned v;

    for(int z = 2; z < 3; z=z+4){
              unsigned long long new_id = 1;
              string partsPath = BASE_PATH + partsDir + partFName + to_string(z);
  //string partsPath = BASE_PATH + partsDir + partFName + to_string(nparts);
  cout <<"\nPartPath: " << partsPath  << "\tfileName " << graphName;
  std::ifstream partFile(partsPath.c_str());
  // partInFile.open(partsPath.c_str());
   //assert(partInFile.is_open());
   unsigned long long p = 1;
     unsigned long long v;
  
     // READ part file and match *******
  //   part[p] = 0;
    // ++p;
   while (partFile >> v){
    // if(p >= num_vertices) break;
    //  cout <<"\n part " << v << " p " << p << endl;
       part[p] = v;
      ++p;
   }
   partFile.close();
     /*
    while (p <= num_vertices) {
       partInFile >> v;
      cout <<"\n part " << v << " p " << p << endl;
         part[p] = v;
      p++;
    }*/
  for(idx_t src=0; src < nparts; ++src)
  {
  //  cout << "\n src: " << src << endl;
    for(unsigned long long i=1; i<num_vertices; ++i){
      if(part[i] == src)   // match the parts read with the src (number of parts)
      {
        vertexToNewVertex[i] = new_id;  //mapping new id at index i
  //    cout <<"\n i: " << i << " part[i]: " <<part[i] << " new_id: " << new_id << endl;
  //    cout <<  "\nvtoNewV: " << vertexToNewVertex[i] << endl;
        newVertexToVertex[new_id] = i;  //stores actual vertex
  //    cout <<"\n ** newVtoV: " << newVertexToVertex[new_id] << endl;
        ++new_id;
      }
     // ++i;
    }
    part_boundaries[src] = new_id;
  }

  for(unsigned long long i=1; i<num_vertices; ++i)
  {
    set<unsigned long long>::iterator it = ogEdgeLists[i].edges.begin();
    set<unsigned long long> new_set;
    while(it != ogEdgeLists[i].edges.end())
    {
      new_set.insert(vertexToNewVertex[*it]);
//      cout <<"\ni: " << i << "\t *it: " << *it << endl;
//      cout <<"\n swapping " << *it << " with " <<  vertexToNewVertex[*it] << endl;
      ++it;
    }
    ogEdgeLists[i].edges.clear();
    ogEdgeLists[i].edges.swap(new_set);
  }

  //string transformedGraphName = graphName + suffix + to_string(z) + ".newGraph";
  string transformedGraphName = graphName + ".newGraph";
  string transPath = BASE_PATH + partsDir + transformedGraphName;
  cout <<"\ntransPath: " << transPath  << "\n";
  ofstream transformedGraphFile;
  transformedGraphFile.open(transPath.c_str());
  for(unsigned long long i=1; i<num_vertices; ++i)
  {
 // cout <<"\nGoing to for loop " << "\n";
    set<unsigned long long>::iterator it = ogEdgeLists[newVertexToVertex[i]].edges.begin();
    while(it != ogEdgeLists[newVertexToVertex[i]].edges.end())
    {
      transformedGraphFile << i << " " << (*it)+1 << endl;
   //   cout <<"\n" << i << " " << *it << endl;
      ++it; 
    }
  }

  transformedGraphFile.close();

  string srcPath = BASE_PATH + sourceName;
  ofstream srcFile;
  srcFile.open(srcPath.c_str());
  srcFile << vertexToNewVertex[SOURCE] << endl;
  srcFile.close();

  string boundsPath = BASE_PATH + boundariesName;
  ofstream boundariesFile;
  boundariesFile.open(boundsPath.c_str());
  for(int i=0; i<nparts; ++i)
    boundariesFile << part_boundaries[i] << endl;
  boundariesFile.close();
 }
  delete[] ogEdgeLists;
  delete[] part;
  delete[] vertexToNewVertex;
  delete[] newVertexToVertex;
  delete[] part_boundaries;
}
