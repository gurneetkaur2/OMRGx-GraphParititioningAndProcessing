#include "util.h"
#include "infinimem/fileIO.h"
#include "mapwriter.h"

// Map, Reduce(Shuffle, Sort, Reduce)
//  - https://developer.yahoo.com/hadoop/tutorial/module4.html#dataflow
// From the Definitive Guide: 
//  - https://books.google.com/books?id=drbI_aro20oC&lpg=PA208&ots=t_xmwdgWg5&dq=hadoop%20sorting%20and%20shuffling%20the%20definitive%20guide&pg=PA208#v=onepage&q&f=true

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
    virtual void* map(const unsigned tid, const unsigned fileId, const std::string& input) = 0;
    virtual void* afterMap(const unsigned tid) { };
    virtual void* beforeReduce(const unsigned tid) { };
    virtual void* reduce(const unsigned tid, const KeyType& key, const std::vector<ValueType>& values) = 0; 
    virtual void* afterReduce(const unsigned tid) { };
     
    // System provided default; overridable by user
    virtual void run();
    void setInput(const std::string infile);
    void setMappers(const unsigned mappers);
    void setReducers(const unsigned reducers);
    void setBatchSize(const unsigned batchSize);     //GK
    void setkItems(const unsigned kBItems);     //GK
    void setGB(const unsigned g);
    void init();
    void writeBuf(const unsigned tid, const KeyType& key, const ValueType& value);  //GK
    //bool read(const unsigned tid, MapBuffer<KeyType, ValueType>& container, std::vector<int>& keysPerBatch, MapBuffer<KeyType, unsigned>& lookUpTable, std::queue<int>& fetchBatchIds);  //GK
    bool read(const unsigned tid);
    void readInit(const unsigned buffer);
    void subtractReduceTimes(const unsigned tid, const double stime);

    // Variables. Ideally, make these private and provide getters/setters.
    unsigned nMappers;
    unsigned nReducers;
    unsigned batchSize;  //Number of items in a batch
    unsigned kBItems;  //Top-k items to be fetched from in memory map
    unsigned gb;
    std::vector<double> map_times;
    std::vector<double> reduce_times;

    std::vector<std::string> fileList;

    pthread_barrier_t barMap;
    pthread_barrier_t barReduce;

    friend void* doMap<KeyType, ValueType>(void* arg);
    friend void* doReduce<KeyType, ValueType>(void* arg);
    friend void* doInMemoryReduce<KeyType, ValueType>(void* arg);

  private:
    // Variables
    std::string inputFolder;
    MapWriter<KeyType, ValueType> writer;
};

