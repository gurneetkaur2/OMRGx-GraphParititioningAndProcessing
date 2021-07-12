#include "util.h"
#include "infinimem/fileIO.h"
#include "mapwriter.h"
#include <climits>
//#include <unistd.h>
#define UINT_MAX 65535

// Map, Reduce(Shuffle, Sort, Reduce)
//  - https://developer.yahoo.com/hadoop/tutorial/module4.html#dataflow
// From the Definitive Guide: 
//  - https://books.google.com/books?id=drbI_aro20oC&lpg=PA208&ots=t_xmwdgWg5&dq=hadoop%20sorting%20and%20shuffling%20the%20definitive%20guide&pg=PA208#v=onepage&q&f=true

__thread bool don;

template <typename KeyType, typename ValueType>
void* doMap(void* arg);

template <typename KeyType, typename ValueType>
void* doReduce(void* arg);

template <typename KeyType, typename ValueType>
void* doInMemoryReduce(void* arg);

template <typename KeyType, typename ValueType>
class MapReduce
{
  public:
    // MUST be provided by the user (can the loadAndPartition be system provided?)
    // TODO: Provide more generic method signatures?
    // TODO: Virtual methods cannot be templatized in C++. Need a better way to avoid this tangle.
    //       Ideally, we need a templatized method that should be user-provided --
    //       templatized so map can work on different data types for different programs
    //       and also helps with write(), read() and sort() etc.
    // TODO: Setup timers
    virtual void* beforeMap(const unsigned tid) { };
    //virtual void* map(const unsigned tid, const unsigned fileId, const std::string& input) = 0;
    virtual void* map(const unsigned tid, const std::string& input, const unsigned lineId) = 0;
    virtual void* afterMap(const unsigned tid) { };
    virtual void* beforeReduce(const unsigned tid) { };
 #ifdef USE_GOMR  
    virtual void* reduce(const unsigned tid, const InMemoryContainer<KeyType, ValueType>& container) = 0;
 #else
    virtual void* reduce(const unsigned tid, const KeyType& key, const std::vector<ValueType>& values) = 0; 
 #endif
    virtual void* updateReduceIter(const unsigned tid) { };
    virtual void* afterReduce(const unsigned tid) { };
    virtual InMemoryContainer<KeyType, ValueType>& readAfterReduce(const unsigned tid, const InMemoryContainer<KeyType, ValueType>& container) { };
    virtual void* writeAfterReduce(const unsigned tid, const InMemoryContainer<KeyType, ValueType>& container) { };
     
    // System provided default; overridable by user
    virtual void run();
/*    void setInput(const std::string infile);
    void setMappers(const unsigned mappers);
    void setReducers(const unsigned reducers);
    void setBatchSize(const unsigned batchSize);     //GK
    void setkItems(const unsigned kBItems);     //GK
    void setGB(const unsigned g);
  */
    void init(const std::string input, const unsigned g, const unsigned mappers, const unsigned reducers, const unsigned vertices, const unsigned bSize, const unsigned kItems, const unsigned iterations);
    void writeBuf(const unsigned tid, const KeyType& key, const ValueType& value);  //GK
    //bool read(const unsigned tid, MapBuffer<KeyType, ValueType>& container, std::vector<int>& keysPerBatch, MapBuffer<KeyType, unsigned>& lookUpTable, std::queue<int>& fetchBatchIds);  //GK
    bool read(const unsigned tid);
    void readInit(const unsigned buffer);
    void cWrite(const unsigned tid);
//, unsigned noItems, InMemoryContainerConstIterator<KeyType, ValueType> end);
    void subtractReduceTimes(const unsigned tid, const double stime);
   // bool getDone(const unsigned tid){
    bool getDone(const unsigned tid) { return don; }
  //  inline static void done() { don = true; }
    void notDone(const unsigned tid){ don = false; }
    inline unsigned int getIterations() { return nIterations; }
void partitionInputForParallelReads();

    // Variables. Ideally, make these private and provide getters/setters.
    unsigned nVertices;
    unsigned nIterations;
    unsigned nMappers;
    unsigned nReducers;
    unsigned batchSize;  //Number of items in a batch
    unsigned kBItems;  //Top-k items to be fetched from in memory map
    unsigned gb;
    IdType numLines;
    std::vector<double> map_times;
    std::vector<double> reduce_times;
    std::vector<unsigned> end_read;

    std::vector<std::string> fileList;

    pthread_barrier_t barMap;
    pthread_barrier_t barReduce;

    friend void* doMap<KeyType, ValueType>(void* arg);
    friend void* doReduce<KeyType, ValueType>(void* arg);
    friend void* doInMemoryReduce<KeyType, ValueType>(void* arg);

  protected:

  private:
    // Variables
    std::string inputFolder;
    std::string inputFileName;
        size_t bytesPerFile;
        size_t linesPerThread;
    //static unsigned int nIterations;
   // static bool done;
    MapWriter<KeyType, ValueType> writer;


};

