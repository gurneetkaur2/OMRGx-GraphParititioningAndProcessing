#ifndef __MAPWRITER_H__
#define __MAPWRITER_H__
#include "infinimem/fileIO.h"
#include <utility> //GK
#include <map>
#include <set>
#include <vector>
#include <stack>
#include <queue>

#ifdef USE_STRING_HASH
#define hashKey(str) stringHash(str)
#endif

#ifdef USE_NUMERICAL_HASH
#define hashKey(number) (number)
#endif

//-*-*-*-*-
template <typename KeyType, typename ValueType>
using InMemoryContainer = std::map<KeyType, std::vector<ValueType> >;

template <typename KeyType, typename ValueType>
using InMemoryContainerIterator = typename InMemoryContainer<KeyType, ValueType>::iterator; 

template <typename KeyType, typename ValueType>
using InMemoryContainerConstIterator = typename InMemoryContainer<KeyType, ValueType>::const_iterator;
//-*-*-*-*-

template <typename KeyType>
using LookUpTable = std::map<KeyType, std::vector<unsigned> >;
#ifdef USE_GOMR
std::vector<unsigned long long> nNbrs;
typedef std::map<unsigned, unsigned > InMemTable;
//std::vector<bool> done;
#endif

std::vector<double> writeBuf_times;
std::vector<double> flushResidues_times;
std::vector<double> infinimem_read_times;
std::vector<double> infinimem_write_times;
std::vector<uint64_t> localCombinedPairs; 

template <typename KeyType, typename ValueType>
void* combine(const KeyType& key, std::vector<ValueType>& to, const std::vector<ValueType>& from);

template <typename KeyType, typename ValueType>
class InMemoryReductionState {
  public:
  std::vector<InMemoryContainerConstIterator<KeyType, ValueType> > begins;
  std::vector<InMemoryContainerConstIterator<KeyType, ValueType> > ends;

  InMemoryReductionState(unsigned size) : begins(size), ends(size) { }
};

template <typename KeyType, typename ValueType>
class MapWriter
{
  public:
    void initBuf(unsigned nMappers, unsigned nReducers, unsigned nVertices, unsigned bSize, unsigned kItems);
    void writeInit();
    void writeBuf(const unsigned tid, const KeyType& key, const ValueType& value); //GK
    void flushMapResidues(const unsigned tid);

    unsigned long long merge(InMemoryContainer<KeyType, ValueType>& toMap, unsigned whichMap, unsigned tid, InMemoryContainerIterator<KeyType, ValueType>& begin, InMemoryContainerConstIterator<KeyType, ValueType> end);
    void betterWriteToInfinimem(const unsigned buffer, const IdType startKey, unsigned noItems, InMemoryContainerConstIterator<KeyType, ValueType> begin, InMemoryContainerConstIterator<KeyType, ValueType> end);

    void performWrite(const unsigned tid, const unsigned buffer, const KeyType& key, const ValueType& value); //GK
    void writeToInfinimem(const unsigned buffer, const IdType startKey, unsigned nItems, const InMemoryContainer<KeyType, ValueType>& inMemMap); //GK
    bool read(const unsigned tid);
    void readInit(const unsigned tid);
    void readClear(const unsigned tid);
    void releaseMapStructures();
    void shutdown();

    //bool don;
    bool getWrittenToDisk() { return writtenToDisk; }
   // bool getDone(const unsigned tid) { return don; }
    //static inline void done() { don = true; }
    //void notDone(const unsigned tid) {
      //     don = false;
    // }

    InMemoryReductionState<KeyType, ValueType> initiateInMemoryReduce(unsigned tid);
    bool getNextMinKey(InMemoryReductionState<KeyType, ValueType>* state, InMemoryContainer<KeyType, ValueType>* record);

    InMemoryContainer<KeyType, ValueType>* readBufMap;
    LookUpTable<KeyType>* lookUpTable;
    std::set<unsigned>* fetchBatchIds;

    std::vector<unsigned long long>* readNextInBatch;
    std::vector<bool>* batchesCompleted;
    std::vector<unsigned>* keysPerBatch;

  private:
    bool read(const unsigned tid, InMemoryContainer<KeyType, ValueType>& readBufMap, std::vector<unsigned>& keysPerBatch, LookUpTable<KeyType>& lookUpTable, std::set<unsigned>& fetchBatchIds, std::vector<unsigned long long>& readNextInBatch, std::vector<bool>& batchesCompleted);
    
    unsigned nVtces;
    unsigned nRows;
    unsigned nCols;
    unsigned batchSize;  //GK
    unsigned kBItems;  //GK
    bool firstInit;
    //IdType* cTotalKeys; //GK
    IdType* nItems; //GK
    InMemoryContainer<KeyType, ValueType>* outBufMap;  //GK
//    std::vector<unsigned>* prev;
//    std::vector<unsigned>* next;

    IdType* totalKeysInFile;
    std::vector<pthread_mutex_t> locks;
    FileIO<RecordType> *io;  //GK
    bool writtenToDisk;
};

#endif // __MAPWRITER_H__

