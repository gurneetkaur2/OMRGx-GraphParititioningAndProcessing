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

static uint64_t countTotalWords = 0;
pthread_mutex_t countTotal;
//-------------------------------------------------
// WordCount walk-through: 
// test- makes no 

//  - http://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Walk-through

template <typename KeyType, typename ValueType>
class PageRank : public MapReduce<KeyType, ValueType>
{
  static thread_local uint64_t countThreadWords;

  public:
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

    void* reduce(const unsigned tid, const KeyType& key, const std::vector<ValueType>& values) {
      //countThreadWords += std::accumulate(values.begin(), values.end(), 0);
      return NULL;
    }

    void* afterReduce(const unsigned tid) {
      //fprintf(stderr, "countThreadWords = %llu\n", countThreadWords);
      return NULL;
    }
};


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
  PageRank<unsigned, unsigned> pr;

  std::string select = "";
  while(true){
    std::cout << "Please select `GOMR` for graph Processing or `OMR` for regular MR application" << std::endl;
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
    std::cout << "Usage: " << argv[0] << " <folderpath> <gb> <nmappers> <nreducers> <nvertices> <batchsize> <kitems>" << std::endl;
         
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
      nvertices = atoi(argv[5]);

  int batchSize = atoi(argv[6]);
  int kitems = atoi(argv[7]);
  
  assert(batchSize > 0);
  pthread_mutex_init(&countTotal, NULL);


  pr.init(folderpath, gb, nmappers, nreducers, nvertices, batchSize, kitems);

  double runTime = -getTimer();
  pr.run(); 
  runTime += getTimer();
  
  std::cout << "Main::Run time : " << runTime << " (msec)" << std::endl;
  std::cout << "Total words: " << countTotalWords << std::endl;

  return 0;
}

