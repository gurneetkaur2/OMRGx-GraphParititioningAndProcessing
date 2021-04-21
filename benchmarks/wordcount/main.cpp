#define MAX_WORD_SIZE 32
#define MAX_REAL_WORD_SIZE 32

#ifdef USE_ONE_PHASE_IO
#include "recordtype.h"
#else
#include "data.pb.h"
#endif

#define USE_STRING_HASH

#include "../../engine/mapreduce.hpp"
#include <iostream>
#include <sstream>
#include <fstream>
#include <string>
#include "pthread.h"
#include <ctime>
#include <cstdlib>

static uint64_t countTotalWords = 0;
pthread_mutex_t countTotal;
//-------------------------------------------------
// WordCount walk-through: 
// test- makes no 

//  - http://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Walk-through

template <typename KeyType, typename ValueType>
class WordCount : public MapReduce<KeyType, ValueType>
{
  static thread_local uint64_t countThreadWords;

  public:
    void* map(const unsigned tid, const unsigned fileId, const std::string& input)
    {
      std::stringstream inputStream(input);
      std::string token;

      while (inputStream >> token) {
#ifdef USE_ONE_PHASE_IO
        unsigned idx = 0;
        while(idx < token.size()) {
          std::string miniToken = token.substr(idx, MAX_REAL_WORD_SIZE);
          this->writeBuf(tid, miniToken, 1);
          idx += (MAX_REAL_WORD_SIZE);
        } 
#else
        this->writeBuf(tid, token, 1);
#endif
      }
      
      return NULL;
    }

    void* reduce(const unsigned tid, const KeyType& key, const std::vector<ValueType>& values) {
      countThreadWords += std::accumulate(values.begin(), values.end(), 0);
      return NULL;
    }

    void* afterReduce(const unsigned tid) {
      //fprintf(stderr, "countThreadWords = %llu\n", countThreadWords);
      pthread_mutex_lock(&countTotal);
      countTotalWords += countThreadWords;
      pthread_mutex_unlock(&countTotal); 
      return NULL;
    }
};

template <typename KeyType, typename ValueType>
thread_local uint64_t WordCount<KeyType, ValueType>::countThreadWords = 0;

template <typename KeyType, typename ValueType>
void* combine(const KeyType& key, std::vector<ValueType>& to, const std::vector<ValueType>& from) {
  assert(to.size() == 1);
  assert(from.size() == 1);
  to[0] += from[0];
  return NULL;
}

//-------------------------------------------------
int main(int argc, char** argv)
{
  WordCount<std::string, unsigned> wc;

  std::string select = "";
  while(true){
    std::cout << "Please select `GOMR` for graph Processing using OMR or `OMR` for regular MR application" << std::endl;
    std::cin >> select;
    if (select == "OMR" | select == "GOMR")
        break;
  }
  std::cout << std::endl;

  if (select == "OMR" && argc != 7)
  {
    std::cout << "Usage: " << argv[0] << " <folderpath> <gb> <nmappers> <nreducers> <batchsize> <kitems>" << std::endl;
    return 0;
   }
   else if (select == "GOMR" && argc != 8) { 
    std::cout << "Usage: " << argv[0] << " <folderpath> <gb> <nmappers> <nreducers> <batchsize> <kitems> <nvertices>" << std::endl;
         
    return 0;
  }

  std::string folderpath = argv[1];
  int gb = atoi(argv[2]);
  int nmappers = atoi(argv[3]);
  int nreducers = atoi(argv[4]);
  int nvertices;
  if (select == "OMR")
      nvertices = -1;
  else
      nvertices = atoi(argv[7]);

  int batchSize = atoi(argv[5]);
  int kitems = atoi(argv[6]);
  
  assert(batchSize > 0);
  pthread_mutex_init(&countTotal, NULL);

//  wc.setInput(folderpath);

  //wc.setThreads(numThreads);   //GK
//  wc.setMappers(nmappers);
 // wc.setReducers(nreducers);
  // Set in-memory map size for a batch; size 3 means 4 items per batch; should be double the k items to be fetched per batch to avoid reading same elements again from infinimem
 // wc.setBatchSize(batchSize);
  //number of items to be fetched from infinimem per batch
 // wc.setkItems(kitems);
 // wc.setGB(gb); 

  std::cout<<"\ngoing to init ";
  wc.init(folderpath, gb, nmappers, nreducers, nvertices, batchSize, kitems);

  double runTime = -getTimer();
  wc.run(); 
  runTime += getTimer();
  
  std::cout << "Main::Run time : " << runTime << " (msec)" << std::endl;
  std::cout << "Total words: " << countTotalWords << std::endl;

  return 0;
}

