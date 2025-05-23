#include "data.pb.h"

#define USE_NUMERICAL_HASH

#include "../../engine/mapreduce.hpp"
#include <iostream>
#include <sstream>
#include <fstream>
#include <string>
#include "pthread.h"
#include <ctime>
#include <cstdlib>
#include<mutex>

int nvertices;
int nmappers;
std::mutex m;
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

//  std::vector<unsigned> nNbrs; // = (nvertices, -1);
  public:

  void* beforeMap(const unsigned tid) {
    return NULL;
  }

  void writeInit(unsigned nCols, unsigned nVtces){
   /* prev = new std::vector<double>[nCols];
    next = new std::vector<double>[nCols];
    //nNbrs = new std::vector<unsigned>[nCols];
   // fprintf(stderr, "TID: %d, nvert:  %d \n", tid, nvertices);
    for(unsigned i=0; i<nCols; i++){
      for (unsigned j = 0; j<=nVtces; ++j) {
        prev[i].push_back(1.0/nvertices);
        next[i].push_back(1.0/nvertices);
//        nNbrs.push_back(0);
      //fprintf(stderr, "\n TID: %d, BEFORE key: %d prev: %f next: %f rank: %.2f \n", tid, i, prev[tid][i], next[tid][i], (1/nvertices));
      }
    }
  //    for (unsigned j = 0; j<=nVtces; ++j) 
 //       nNbrs.push_back(0);
 */ }

 unsigned setPartitionId(const unsigned tid)
   {
    unsigned nCols = this->getCols();
             //fprintf(stderr,"\nTID: %d writing to partition: %d " , tid, tid % nCols);
    return  -1; //tid % nCols;
   }


  void* map(const unsigned tid, const unsigned fileId, const std::string& input, const unsigned nbufferId, const unsigned hiDegree)
  {
    std::stringstream inputStream(input);
    unsigned to, token;
    std::vector<unsigned> from;

    inputStream >> to;
    while(inputStream >> token){
      from.push_back(token);
    }
    unsigned nCols = this->getCols();

   // prev[tid%nCols].at(to) = 1.0/from.size();
   /* if(nNbrs.at(to) == 0){
      auto my_lock = std::unique_lock<std::mutex>(m);
      nNbrs.at(to) = from.size();
    }*/

    for(unsigned i = 0; i < from.size(); ++i){
      //                fprintf(stderr,"\nVID: %d FROM: %zu size: %zu", to, from[i], from.size());
      Edge e;
      e.src = from[i];
      e.dst = to;
      e.rank = 1.0/nvertices;
      e.vRank = 1.0/nvertices;
      e.numNeighbors = from.size();
      this->writeBuf(tid, to, e, nbufferId, from.size());
      //this->writeBuf(tid, to, from[i], nbufferId, from.size());
    }

    return NULL;
  }

  void* beforeReduce(const unsigned tid) {
      //  unsigned int iters = 0;
  }

  void* reduce(const unsigned tid, const InMemoryContainer<KeyType, ValueType>& container){
    long double sum = 0.0;
       // iterate each vertex neighbor in adjlist
   // fprintf(stderr, "TID: %d, Reducing values \n", tid);
   std::vector<Edge *> vertices;
   IdType v;
   for(auto it = container.begin(); it != container.end(); it++){ 
      unsigned key = it->first;
//      for(auto vit = it->second.begin(); vit != it->second.end(); ++vit) { 
        for(int k=0; k<it->second.size(); k++) {
         /*float val = it->second[k].rank;
         unsigned nnbrs = it->second[k].numNeighbors;
         if(nnbrs > 0)
            sum += val/nnbrs;
         */
         Edge e = it->second[k];
         v = e.dst;
         vertices.push_back(&e);
         while(k<it->second.size() && v==e.dst) k++; 
         //float val = prev[tid].at(*vit);
         //sum += val;
        }
   }
     //long double old = *vit.rank;
     //next[tid].at(key) = (1-DAMPING_FACTOR) + (DAMPING_FACTOR*sum);
     for(unsigned i= 0; i<vertices.size(); ) {
        IdType dst = vertices[i]->dst;
        long double sum = 0.0;
        // process neighbors
        unsigned dstStartIndex = i;
        while(i<vertices.size() && dst == vertices[i]->dst){
          if(vertices[i]->numNeighbors > 0){
            sum += (vertices[i]->rank / vertices[i]->numNeighbors);
          }
          i++;
        }
        float rank = (DAMPING_FACTOR * sum) + (1 - DAMPING_FACTOR);
        unsigned dstEndIndex = i;
        for(unsigned dstIndex = dstStartIndex; dstIndex<dstEndIndex; dstIndex++){
            vertices[dstIndex]->vRank = rank;
        }
     }
    //for(auto it = values.begin(); it != values.end(); ++it) { 
      //for(auto vit = it->second.begin(); vit != it->second.end(); ++vit) { 
      /* for(int k=0; k<it->second.size(); k++) {
       // fprintf(stderr, "TID: %d, Prev: %f Neighbor %d \n", tid, prev[tid].at(*it), nNbrs.at(*it));
         unsigned nnbrs = it->second[k].numNeighbors;
         if(nnbrs > 0) { 
          it->second[k].vRank = pagerank;
      }
      else
        efprintf(stderr, "TID: %d, Neighbors of %d not found \n", tid, *it);
    }*/
    //efprintf(stderr, "\n TID: %d, AFTER Key: %d prev: %f next: %f \n", tid, key, prev[tid].at(key), next[tid].at(key));
   // fprintf(stderr, "TID: %d, DONE Reducing key prev: %d, next: %d \n", tid, prev[tid].at(key), next[tid].at(key));

    //next[tid].at(key) = pagerank;
   // it->first.vRank = pagerank;
    //below code to be used for self convergence of pgrank
  /*  if( fabs(old - next[tid].at(*it)) > TOLERANCE ){
      //  fprintf(stderr, "TID: %d, Still not done: %d  \n", tid, this->getDone(tid));
                        // flag to iterate records again--  
                        this->notDone(tid);
                        done.at(tid) = 0; 
     }*/
 // }
    return NULL;
  }

  void* updateReduceIter(const unsigned tid) {

    ++iteration;
    //fprintf(stderr, "AfterReduce\n");
     // assign next to prev for the next iteration , copy to prev of all threads
   //  iters = iters + 1;
    if(iteration >= this->getIterations()){
      don = true;
      return NULL;
    }
     //prev[tid].assign(next[tid].begin(), next[tid].end());
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
  PageRank<IdType, Edge> pr;

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
int hiDegree;
//int nvertices;
//  if (select == "OMR")
//    nvertices = -1;
#ifdef USE_GOMR
nvertices = atoi(argv[7]);
niterations = atoi(argv[8]);
if(atoi(argv[9]) > 0)
  hiDegree = atoi(argv[9]);
else
hiDegree = 0;

#else
nvertices = -1;
niterations = 1;
#endif

int batchSize = atoi(argv[5]);
int kitems = atoi(argv[6]);

assert(batchSize > 0);
//pthread_mutex_init(&countTotal, NULL);


pr.init(folderpath, gb, nmappers, nreducers, nvertices, hiDegree, batchSize, kitems, niterations);

pr.writeInit(nreducers, nvertices);
double runTime = -getTimer();
pr.run(); 
runTime += getTimer();

std::cout << "Main::Run time : " << runTime << " (msec)" << std::endl;
//std::cout << "Total words: " << countTotalWords << std::endl;
return 0;
}

