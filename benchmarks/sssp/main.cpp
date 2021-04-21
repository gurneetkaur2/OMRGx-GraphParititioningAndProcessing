#ifdef USE_ONE_PHASE_IO
#include "recordtype.h"
#else
#include "data.pb.h"
#endif

#define USE_NUMERICAL_HASH
#define MAX_VERTEX_VALUE (ULLONG_MAX)

#include "../../engine/mapreduce.hpp"
#include <iostream>
#include <sstream>
#include <fstream>
#include <string>
#include "pthread.h"
#include <ctime>
#include <cstdlib>
#include <fstream>

int nvertices;
int nmappers;
//-------------------------------------------------
// WordCount walk-through: 
// test- makes no 

//  - http://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Walk-through
__thread unsigned iteration = 0;
unsigned SOURCE = 1;
template <typename KeyType, typename ValueType>
class Sssp : public MapReduce<KeyType, ValueType>
{
  // static thread_local uint64_t countThreadWords;
  //static thread_local std::vector<unsigned> prev; // = (nvertices, -1);
  //static thread_local std::vector<unsigned> next; // = (nvertices, -1);

  std::vector<unsigned long>* dist; // = (nvertices, -1);// store distance values for the vertices
  std::vector<unsigned long> gdist; // = (nvertices, -1);// store distance values for the vertices
  std::vector<unsigned long> ewt; //(nvertices, 1);
  public:

  void* beforeMap(const unsigned tid) {
    dist = new std::vector<unsigned long>[nmappers];
  //  ewt = new std::vector<unsigned long>[nmappers];
    fprintf(stderr, "TID: %d, nvert:  %d \n", tid, nvertices);
    for (unsigned i = 0; i<=nvertices; ++i) {
      dist[tid].push_back(MAX_VERTEX_VALUE);
      if(tid == 0){
        gdist.push_back(MAX_VERTEX_VALUE);
        ewt.push_back(1);
      }
    //  fprintf(stderr, "\n TID: %d, BEFORE key: %d dist: %f ewt: %f rank: %.2f \n", tid, i, dist[tid][i], ewt[tid][i], (1/nvertices));
    }
     //fprintf(stderr, "TID: %d, Before Mapping values \n", tid);
    gdist.at(0) = 0;
    gdist.at(1) = 0;
    dist[0][0] = 0;
    dist[0][1] = 0;
    return NULL;
  }
  void* map(const unsigned tid, const unsigned fileId, const std::string& input)
  {
    std::stringstream inputStream(input);
    unsigned to, token;
    std::vector<unsigned> from;

    inputStream >> to;
    while(inputStream >> token){
      from.push_back(token);
    }
    for(unsigned i = 0; i < from.size(); ++i){
      //                fprintf(stderr,"\nVID: %d FROM: %zu size: %zu", to, from[i], from.size());
      this->writeBuf(tid, to, from[i]);
    }

    return NULL;
  }

  void* beforeReduce(const unsigned tid) {
    //  unsigned int iters = 0;
  }
  
  void* reduce(const unsigned tid, const KeyType& key, const std::vector<ValueType>& values) {
    //countThreadWords += std::accumulate(values.begin(), values.end(), 0);
    //long double sum = 0.0;
    // iterate each vertex neighbor in adjlist
    // fprintf(stderr, "TID: %d, Reducing values \n", tid);
    for(auto it = values.begin(); it != values.end(); ++it) { 
      // check if the fetched neighbor has its adjlist
      //need adjlist size for each neighbor 
      // need to know number of neighbors for each value???? 
      //  fprintf(stderr, "TID: %d checking values it: %d \n", tid, *it);
      // fprintf(stderr, "TID: %d, Prev: %f Neighbor %d \n", tid, prev[tid].at(*it), nNbrs.at(*it));
      int oldDistV = gdist.at(key);
      int oldDistN = gdist.at(*it);
      if(oldDistN != MAX_VERTEX_VALUE){
        if(oldDistV > oldDistN + ewt[*it]) { // '1' is assuming single edge weight for all edges
           dist[tid].at(key) = oldDistN + ewt[*it];  //TODO: should the distance for *it be updated instead? 
          this->notDone(tid);
          fprintf(stderr,"\nTID: %d getDone: %d ", tid, this->getDone(tid));
        }
      }
    /*  else{
          dist[tid].at(*it) = oldDistN + ewt[*it];  //TODO: should the distance for *it be updated instead? 
      }*/
    }
// fprintf(stderr, "TID: %d, DONE Reducing key prev: %d, next: %d \n", tid, prev[tid].at(key), next[tid].at(key));
fprintf(stderr, "\n TID: %d, AFTER Key: %d dist: %u ewt: %u \n", tid, key, dist[tid].at(key), ewt.at(key));

return NULL;
}

void* updateReduceIter(const unsigned tid) {
  ++iteration;
  //fprintf(stderr, "AfterReduce\n");
  // assign next to prev for the next iteration , copy to prev of all threads
  //  iters = iters + 1;
  // prev[tid].assign(next[tid].begin(), next[tid].end());
    for (auto i = 0; i < dist[tid].size(); i++){
      if(dist[tid][i] != MAX_VERTEX_VALUE && dist[tid][i] < gdist[i])
        gdist[i] = dist[tid][i];
  //  fprintf(stderr, "\n TID: %d, prev: %f next: %f \n", tid, prev[tid][i], next[tid][i]);
   }
  std::ofstream vfile;
  char fname[20];
  sprintf(fname, "kverifier"+tid);
  vfile.open (fname);
  for(IdType i=0; i<=nvertices; ++i) {
      vfile << "Node: " << i << " Distance: " << dist[tid].at(i) << std::endl;
  }
   vfile.close();

  fprintf(stderr,"\nTID: %d, iteration: %d ----", tid, iteration);
  //fprintf(stderr, "pagerank: thread %u iteration %d took %.3lf ms to process %llu vertices and %llu edges\n", tid, iteration, timevalToDouble(e) - timevalToDouble(s), nvertices, nedges);
  //}
  return NULL;
  }

  void* afterReduce(const unsigned tid) {

   return NULL;
  }
  
};


template <typename KeyType, typename ValueType>
void* combine(const KeyType& key, std::vector<ValueType>& to, const std::vector<ValueType>& from) {
  to.insert(to.end(), from.begin(), from.end());
  return NULL;
}

//-------------------------------------------------
int main(int argc, char** argv)
{
  Sssp<unsigned, unsigned> pr;

  //  std::string select = "";
  /*  while(true){
      std::cout << "Please select `GOMR` for graph Processing or `OMR` for regular MR application" << std::endl;
      std::cin >> select;
      if (select == "OMR" | select == "GOMR")
      break;
      }*/
  //  std::cout << std::endl;

  if (argc < 8)
  {
    //   std::cout << "Usage: " << argv[0] << " <folderpath> <gb> <nmappers> <nreducers> <batchsize> <kitems>" << std::endl;
    //   return 0;
    // }
    // else if (select == "GOMR" && argc != 9) { 
  std::cout << "Usage: " << argv[0] << " <folderpath> <gb> <nmappers> <nreducers> <batchsize> <kitems> <optional - nvertices> <optional - iters>" << std::endl;

  return 0;
}

std::string folderpath = argv[1];
int gb = atoi(argv[2]);
nmappers = atoi(argv[3]);
int nreducers = atoi(argv[4]);
int niterations = 1;
//int nvertices;
//  if (select == "OMR")
//    nvertices = -1;
#ifdef USE_GOMR
nvertices = atoi(argv[7]);
niterations = atoi(argv[8]);
#else
nvertices = -1;
niterations = 1;
#endif

int batchSize = atoi(argv[5]);
int kitems = atoi(argv[6]);

assert(batchSize > 0);
//pthread_mutex_init(&countTotal, NULL);


pr.init(folderpath, gb, nmappers, nreducers, nvertices, batchSize, kitems, niterations);

double runTime = -getTimer();
pr.run(); 
runTime += getTimer();

std::cout << "Main::Run time : " << runTime << " (msec)" << std::endl;
//std::cout << "Total words: " << countTotalWords << std::endl;
return 0;
}

