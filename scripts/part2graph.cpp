#include <iostream>
#include <fstream>
#include <sstream>
#include <set>
#include <cassert>
#include <cstdlib>
#include <sys/stat.h>
#include <sys/types.h>

//#include "metis.h"

#define BASE_PATH "../inputs/"
#define PARTS_PATH "parts_"

#define SOURCE 0

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
  assert(argc == 4);
  string graphName = argv[1];
  string transformedGraphName = graphName + ".newGraph";
  string adjGraphName = "adj_" + graphName;
  string boundariesName = graphName + ".boundaries";
  // string commName = graphName + ".comm";
  string sourceName = graphName + ".src";
  idx_t nparts = atoi(argv[2]);

  string partsDir = "";
  partsDir += PARTS_PATH;
  partsDir += argv[2];
  partsDir += "/";

  string partFName = argv[3];
 // cout<<"\nParts Dir: "<<partsDir;
  unsigned long long num_edges = 0, num_vertices = 0;
  {
    unsigned long long to, from, edge;
    string graphPath = BASE_PATH + adjGraphName;
    std::ifstream infile(graphPath.c_str());
    infile >> to >> from;
    num_vertices = to;
    num_edges = from;
 //   cout<<"\nADJGRAPH FILE: "<<adjGraphName;
 //   cout<<"\nNUM Vertices: "<<num_vertices <<" NUM EDGES: " << num_edges;
  }
++num_vertices;
//  EdgeList* edgeLists = new EdgeList[num_vertices];
  EdgeList* ogEdgeLists = new EdgeList[num_vertices];
   // cout<<"\nVertices: "<<num_vertices;

  string graphPath = BASE_PATH + graphName;

   // cout<<"\nGRAPH PATH: "<<graphPath;

  std::ifstream infile(graphPath.c_str());
  unsigned long long to, from;

  while (infile >> to >> from ) {
    ogEdgeLists[to].edges.insert(from);
  //  cout<<"\nTO: "<<to <<" FROM: " << from;
  }

  idx_t nvtxs = num_vertices;
  idx_t* part = new idx_t[nvtxs];
  part[0] = 256;
 //   unsigned long long to;
    string partsPath = BASE_PATH + partsDir + partFName;
  cout<<"\nParts Dir: "<<partsPath;
 //   std::ifstream infile(partsPath.c_str());
  //  infile >> to;

  unsigned long long* vertexToNewVertex = new unsigned long long[num_vertices];
  unsigned long long* newVertexToVertex = new unsigned long long[num_vertices];
  unsigned long long new_id = 0;

  unsigned long long* part_boundaries = new unsigned long long[nparts];

  for(idx_t src=0; src < nparts; ++src)
  {
    for(unsigned long long i=0; i<num_vertices; ++i){
     cout <<"\nPart[i]: " <<part[i] << " src: " <<src << " i: " << i;
      if(part[i] == src)
      {
        vertexToNewVertex[i] = new_id;
        newVertexToVertex[new_id] = i;
        ++new_id;
      }
    }
    part_boundaries[src] = new_id;
  }

  for(unsigned long long i=0; i<num_vertices; ++i)
  {
    set<unsigned long long>::iterator it = ogEdgeLists[i].edges.begin();
    set<unsigned long long> new_set;
    while(it != ogEdgeLists[i].edges.end())
    {
      new_set.insert(vertexToNewVertex[*it]);
      ++it;
    }
    ogEdgeLists[i].edges.clear();
    ogEdgeLists[i].edges.swap(new_set);
  }

  string transPath = BASE_PATH + partsDir + transformedGraphName;
  ofstream transformedGraphFile;
  transformedGraphFile.open(transPath.c_str());

    cout<<"\nTransformedGRAPH FILE: "<<transPath;
  for(unsigned long long i=0; i<=num_vertices; ++i)
  {
    set<unsigned long long>::iterator it = ogEdgeLists[newVertexToVertex[i]].edges.begin();
    while(it != ogEdgeLists[newVertexToVertex[i]].edges.end())
    {
      transformedGraphFile << i+1 << " " << (*it)+1 << endl;
    //cout<<"\ni: "<<i <<" it: " << (*it)+1;
      ++it; 
    }
  }

  transformedGraphFile.close();

  string srcPath = BASE_PATH + partsDir + sourceName;
  ofstream srcFile;
    cout<<"\nSRCGRAPH FILE: "<<srcPath;
  srcFile.open(srcPath.c_str());
  srcFile << vertexToNewVertex[SOURCE] << endl;
  srcFile.close();

  string boundsPath = BASE_PATH + partsDir + boundariesName;
  ofstream boundariesFile;
    cout<<"\nBounds FILE: "<<boundsPath;
  boundariesFile.open(boundsPath.c_str());
  for(int i=0; i<nparts; ++i)
    boundariesFile << part_boundaries[i] << endl;
  boundariesFile.close();

  delete[] part;
  delete[] vertexToNewVertex;
  delete[] newVertexToVertex;
  delete[] part_boundaries;
//  delete[] edgeLists;
  delete[] ogEdgeLists;
}
