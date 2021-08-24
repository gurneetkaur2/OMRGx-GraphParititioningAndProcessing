#ifdef USE_ONE_PHASE_IO
#include "recordtype.h"
#else
#include "data.pb.h"
#endif

#define USE_NUMERICAL_HASH

#include "../../engine/mapreduce.hpp"
#include <iostream>
#include <sstream>
#include <fstream>
#include <string>
#include "pthread.h"
#include <ctime>
#include <cstdlib>

int nvertices;
int nmappers;
//static uint64_t countTotalWords = 0;
//pthread_mutex_t countTotal;
const long double DAMPING_FACTOR = 0.85; // Google's recommendation in original paper.
const long double TOLERANCE = (1e-1);
//-------------------------------------------------
// WordCount walk-through: 
// test- makes no 

//  - http://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Walk-through
__thread unsigned iteration = 0;
template <typename KeyType, typename ValueType>
class PageRank : public MapReduce<KeyType, ValueType>
{
 // static thread_local uint64_t countThreadWords;
  //static thread_local std::vector<unsigned> prev; // = (nvertices, -1);
  //static thread_local std::vector<unsigned> next; // = (nvertices, -1);

  std::vector<double>* prev; // = (nvertices, -1);
  std::vector<double>* next; // = (nvertices, -1);
  public:

  void* beforeMap(const unsigned tid) {
    prev = new std::vector<double>[nmappers];
    next = new std::vector<double>[nmappers];
    fprintf(stderr, "TID: %d, nvert:  %d \n", tid, nvertices);
    for (unsigned i = 0; i<=nvertices; ++i) {
        prev[tid].push_back(1.0/nvertices);
        next[tid].push_back(1.0/nvertices);
      fprintf(stderr, "\n TID: %d, BEFORE key: %d prev: %f next: %f rank: %.2f \n", tid, i, prev[tid][i], next[tid][i], (1/nvertices));
    }
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

    prev[tid].at(to) = 1.0/from.size();
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
    long double sum = 0.0;
       // iterate each vertex neighbor in adjlist
   // fprintf(stderr, "TID: %d, Reducing values \n", tid);
    for(auto it = values.begin(); it != values.end(); ++it) { 
        float val = prev[tid].at(*it);
        sum += val;
    }
     long double old = prev[tid].at(key);
     //next[tid].at(key) = (1-DAMPING_FACTOR) + (DAMPING_FACTOR*sum);
     float pagerank = DAMPING_FACTOR + (1 - DAMPING_FACTOR) * sum;

    for(auto it = values.begin(); it != values.end(); ++it) { 
         // check if the fetched neighbor has its adjlist
        //need adjlist size for each neighbor 
        // need to know number of neighbors for each value???? 
      //  fprintf(stderr, "TID: %d checking values it: %d \n", tid, *it);
       // fprintf(stderr, "TID: %d, Prev: %f Neighbor %d \n", tid, prev[tid].at(*it), nNbrs.at(*it));
      if(nNbrs.at(*it) > 0) { 
        next[tid].at(*it) = ( pagerank / nNbrs.at(*it));
      }
      else
        fprintf(stderr, "TID: %d, Neighbors of %d not found \n", tid, *it);
    }
    fprintf(stderr, "\n TID: %d, AFTER Key: %d prev: %f next: %f \n", tid, key, prev[tid].at(key), next[tid].at(key));
   // fprintf(stderr, "TID: %d, DONE Reducing key prev: %d, next: %d \n", tid, prev[tid].at(key), next[tid].at(key));

    next[tid].at(key) = pagerank;

    if(iteration > this->getIterations()){
      done.at(key) = 1;
//      break;
    }
     if( fabs(old - next[tid].at(key)) > TOLERANCE ){
      //  fprintf(stderr, "TID: %d, Still not done: %d  \n", tid, this->getDone(tid));
                        // flag to iterate records again--  
                        this->notDone(key);
                        done.at(key) = 0; 
     }
    return NULL;
  }

  void* updateReduceIter(const unsigned tid) {

    ++iteration;
    //fprintf(stderr, "AfterReduce\n");
     // assign next to prev for the next iteration , copy to prev of all threads
   //  iters = iters + 1;
     prev[tid].assign(next[tid].begin(), next[tid].end());
  //  for (auto i = 0; i < prev[tid].size(); i++){
    //  fprintf(stderr, "\n TID: %d, prev: %f next: %f \n", tid, prev[tid][i], next[tid][i]);
   // }
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
  PageRank<unsigned, unsigned> pr;

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

