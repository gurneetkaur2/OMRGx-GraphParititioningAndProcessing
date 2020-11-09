#include "fileIO.h"
#include "util.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <iostream>

#include <stdio.h>
#include <stdio_ext.h>

#include <errno.h>

// +1 for space after id __AND__ another +1 for newline
#define META_LINE_LENGTH (2*(MAX_CHARS_STREAMOFF + 1))

//----------------------------------------------------------
template<typename T>
#if defined(USE_METIS) || defined(GRAPHCHI)
TwoPhaseFileIO<T>::TwoPhaseFileIO(const char *loc, unsigned threadCount, IdType ePerFile, const std::vector<IdType>& boundaries)
  : FileIO<T>(loc, threadCount, ePerFile, boundaries) {
#else
TwoPhaseFileIO<T>::TwoPhaseFileIO(const char *loc, unsigned threadCount, IdType ePerFile)
  : FileIO<T>(loc, threadCount, ePerFile) {
#endif

  data_ifs = NULL;
  meta_iofs = NULL;
  data_ofs = NULL;

  fprintf(stderr, "Initializing TwoPhaseFileIO\n");
  std::cout << "Number of MRFiles: " << this->nthreads << std::endl;
  
  init();
}

//----------------------------------------------------------
template<typename T>
TwoPhaseFileIO<T>::~TwoPhaseFileIO() {
  shutdown();
}

//----------------------------------------------------------
template<typename T>
void TwoPhaseFileIO<T>::init() {
  /*
  char path[256];
  strcat(strcpy(path, "rm -rf "), this->tmpFilePath()); // Don't assume clean shutdown.
  int t = system(path); assert( t == 0 );

  strcat(strcpy(path, "mkdir -p "), this->tmpFilePath());
  t = system(path);
  */

  fileIoSetup();
}

//----------------------------------------------------------
template<typename T>
void TwoPhaseFileIO<T>::shutdown() {
  char path[256];
  strcat(strcpy(path, "rm -rf "), this->tmpFilePath());
  //system(path); // Ignoring return; okay in this case
}

//----------------------------------------------------------
template<typename T>
void TwoPhaseFileIO<T>::fileIoSetup() {
  char tpath[256];
  strcat(strcpy(tpath, "rm -rf "), this->tmpFilePath()); // Don't assume clean exit.
  int t = system(tpath); assert( t == 0 );

  // This assumption is key to the implementation:
  // When reading the file pointers from meta data file, we need to be able to convert 
  // it to a known numeric type. For instance, atoi or atol or just read in (see _get below)

 // assert(std::numeric_limits<long>::max() == std::numeric_limits<std::streamoff>::max()); //gk

  strcat(strcpy(tpath, "mkdir -p "), this->tmpFilePath());

  char dmpath[256]; 
  strcpy(dmpath, tpath);
  strcat(dmpath, "/data");
  t = system(dmpath);

  strcpy(dmpath, tpath);
  strcat(dmpath, "/meta");
  t = system(dmpath); 

  null_string = (char *) calloc(META_LINE_LENGTH, sizeof(char)); // Also forces contents to be zero

  meta_iofs = (int **)calloc(this->nthreads, sizeof(int *));
  metaFileMutex = (pthread_mutex_t **)calloc(this->nthreads, sizeof(pthread_mutex_t *));

  data_ifs = (int **)calloc(this->nthreads, sizeof(int *));
  data_ofs = (int *)calloc(this->nthreads, sizeof(int));
  dataFileMutex = (pthread_mutex_t *)calloc(this->nthreads, sizeof(pthread_mutex_t));

  assert((data_ofs != NULL) && (meta_iofs != NULL) && (data_ifs != NULL) 
         && (dataFileMutex != NULL) && (metaFileMutex != NULL));

  for(unsigned i=0; i<this->nthreads; i++) {
    char suffix[256]; char path[256];
    sprintf(suffix, "/data/data_%d", i);
    data_ofs[i] = open(strcat(strcpy(path, this->tmpFilePath()), suffix), O_CREAT | O_LARGEFILE | O_TRUNC | O_RDWR, S_IRUSR | S_IWUSR);
    pthread_mutex_init(&dataFileMutex[i], NULL);
    assert(data_ofs[i] > 0);

    metaFileMutex[i] = (pthread_mutex_t *)calloc(PARALLEL_ACCESS, sizeof(pthread_mutex_t));
    meta_iofs[i] = (int *)calloc(PARALLEL_ACCESS, sizeof(int));
    data_ifs[i] = (int *)calloc(PARALLEL_ACCESS, sizeof(int));
    assert((data_ofs[i]) && (meta_iofs[i] != NULL) && (data_ifs[i] != NULL) && (metaFileMutex[i] != NULL));

    for(int j=0; j<PARALLEL_ACCESS; j++) {
      pthread_mutex_init(&metaFileMutex[i][j], NULL);
      sprintf(suffix, "/data/data_%d", i);
      data_ifs[i][j] = open(strcat(strcpy(path, this->tmpFilePath()), suffix), O_LARGEFILE | O_RDONLY);
      assert(data_ifs[i][j] > 0);

      sprintf(suffix, "/meta/meta_%d", i); // Note the i: we open PARALLEL_ACCESS number of FPs to the i-th meta/data files
	meta_iofs[i][j] = open(strcat(strcpy(path, this->tmpFilePath()), suffix), O_CREAT | O_LARGEFILE | O_TRUNC | O_RDWR, S_IRUSR | S_IWUSR);
	assert(meta_iofs[i][j] > 0);
    }
  }
}

//----------------------------------------------------------
template<typename T>
void TwoPhaseFileIO<T>::fileIoDestroy() {
  for(unsigned i=0; i < this->nthreads; i++) {
    pthread_mutex_destroy(&dataFileMutex[i]);
    close(data_ofs[i]);

    for(int j=0; j<PARALLEL_ACCESS; j++) {
      pthread_mutex_destroy(&metaFileMutex[i][j]);
      close(meta_iofs[i][j]);
      close(data_ifs[i][j]);
    }
    free(metaFileMutex[i]);
    free(meta_iofs[i]);
    free(data_ifs[i]);
  }
  free(data_ofs);
  free(data_ifs);
  free(meta_iofs);
  
  free(metaFileMutex);
  free(dataFileMutex);

  free(null_string);
}

//----------------------------------------------------------
// 1. Obtain lock for i-th meta data record
// 2. Fetch pointer to end of data file
// 2. Add meta data to meta_ file
// 3. Append data to data_ file
template<typename T>
  fileIoReturn_t TwoPhaseFileIO<T>::file_set(const unsigned& tid, IdType key, const T& value) {
  timeval s, e;
  gettimeofday(&s, NULL);
  //unsigned tid = this->tid(key);
  unsigned hid = this->hash(key);
  this->individual_write_counts[tid]++;

  // ALWAYS APPEND to data file; when modifying this code, also update else case in _add
  pthread_mutex_lock( &dataFileMutex[tid] );
  off64_t datastart = lseek(data_ofs[tid], 0, SEEK_END); assert(datastart != -1);
  bool good = value.SerializeToFileDescriptor(data_ofs[tid]); assert(good == true);
  pthread_mutex_unlock( &dataFileMutex[tid] );

  pthread_mutex_lock( &metaFileMutex[tid][hid] );
  int err = lseek(meta_iofs[tid][hid], META_LINE_LENGTH * key, SEEK_SET); assert(err != -1);
  char meta[META_LINE_LENGTH]; sprintf(meta, "%019d %019ld", value.ByteSize(), datastart); meta[META_LINE_LENGTH] = '\0';
  err = write(meta_iofs[tid][hid], meta, META_LINE_LENGTH); assert(err != -1);
  pthread_mutex_unlock( &metaFileMutex[tid][hid] );

  gettimeofday(&e, NULL);
  this->individual_write_times[tid] += timevalToDouble(e) - timevalToDouble(s);
  return FILEIO_SUCCESS;
}

//----------------------------------------------------------
// 0. Obtain a lock on appropriate mutex
// 1. Index into meta data file and get data pointer
// 2. seekg() to the right place in the data file & read
template<typename T>
fileIoReturn_t TwoPhaseFileIO<T>::file_get(const unsigned& tid, IdType key, T& value) {
  timeval s, e;
  gettimeofday(&s, NULL);
  //unsigned tid = this->tid(key);
  unsigned hid = this->hash(key);
  this->individual_read_counts[tid]++;

  pthread_mutex_lock( &metaFileMutex[tid][hid] );
  off64_t metastart = lseek(meta_iofs[tid][hid], META_LINE_LENGTH * key, SEEK_SET); assert(metastart != -1);
  char length_str[META_LINE_LENGTH], *datapointer_str;
  int err = read(meta_iofs[tid][hid], length_str, META_LINE_LENGTH); assert(err != -1);
  pthread_mutex_unlock( &metaFileMutex[tid][hid] );
  datapointer_str = length_str + MAX_CHARS_STREAMOFF + 1;
  length_str[MAX_CHARS_STREAMOFF] = datapointer_str[MAX_CHARS_STREAMOFF] = '\0';
  std::streamoff length = atol(length_str);  //gk
  off64_t datapointer = atoll(datapointer_str);

  pthread_mutex_lock( &dataFileMutex[tid] );
  err = lseek(data_ifs[tid][hid], datapointer, SEEK_SET); assert(err != -1);
  assert(datapointer != -1);
  void *buf = malloc(length); assert(buf != NULL);
  err = read(data_ifs[tid][hid], buf, length); assert(err != -1);
  bool good = value.ParseFromArray(buf, length); assert(good == true);
  free(buf);
  pthread_mutex_unlock( &dataFileMutex[tid] );

  gettimeofday(&e, NULL);
  this->individual_read_times[tid] += timevalToDouble(e) - timevalToDouble(s);
  return FILEIO_SUCCESS;
}

//----------------------------------------------------------
// Simply remove the entry from meta data file
template<typename T>
fileIoReturn_t TwoPhaseFileIO<T>::file_delete(const unsigned& tid, IdType key) {
  //unsigned tid = this->tid(key);
  unsigned hid = this->hash(key);

  pthread_mutex_lock( &metaFileMutex[tid][hid] );
  char empty[META_LINE_LENGTH];
  memset(empty, 0, META_LINE_LENGTH);
  int err = lseek(meta_iofs[tid][hid], META_LINE_LENGTH * key, SEEK_SET); assert(err != -1);
  err = write(meta_iofs[tid][hid], empty, META_LINE_LENGTH); assert(err != -1);
  pthread_mutex_unlock( &metaFileMutex[tid][hid] );

  return FILEIO_SUCCESS;
}

//----------------------------------------------------------
// 0. Seek to end of file ONCE and append data items to data_* files in a loop.
// 1. While appending, save datapointer and length to array of strings.
// 2. Write out meta data in one block
template<typename T>
fileIoReturn_t TwoPhaseFileIO<T>::file_set_batch(const unsigned& tid, const IdType& startKey, unsigned nItems, T* arrayOfItems) {
  timeval s, e;
  gettimeofday(&s, NULL);
  //unsigned tid = this->tid(startKey);
  unsigned hid = this->hash(startKey);
  this->batch_write_counts[tid]++;

  char **meta = (char **)calloc(nItems, sizeof(char *));
  for(unsigned i=0; i<nItems; i++) {
    meta[i] = (char *)calloc(1, META_LINE_LENGTH);
    assert(meta[i] != 0);
  }
  // ALWAYS APPEND to data file; when modifying this code, also update else case in _add
  pthread_mutex_lock( &dataFileMutex[tid] );
  off64_t datastart = lseek(data_ofs[tid], 0, SEEK_END); assert(datastart != -1);
  for(unsigned i=startKey; i<startKey+nItems; i++) {
    bool good = arrayOfItems[i-startKey].SerializeToFileDescriptor(data_ofs[tid]); assert(good == true);
    sprintf(meta[i-startKey], "%019d %019ld", arrayOfItems[i-startKey].ByteSize(), datastart); meta[i-startKey][META_LINE_LENGTH] = '\0';
    datastart = lseek(data_ofs[tid], 0, SEEK_CUR); assert(datastart != -1);
  }
  pthread_mutex_unlock( &dataFileMutex[tid] );

  pthread_mutex_lock( &metaFileMutex[tid][hid] );
  int err = lseek(meta_iofs[tid][hid], META_LINE_LENGTH * startKey, SEEK_SET); assert(err != -1);
  for(unsigned i=startKey; i<startKey+nItems; i++) {
    err = write(meta_iofs[tid][hid], meta[i-startKey], META_LINE_LENGTH); assert(err != -1);
  }
  pthread_mutex_unlock( &metaFileMutex[tid][hid] );

  for(unsigned i=0; i<nItems; i++)
    free(meta[i]);
  free(meta);

  gettimeofday(&e, NULL);
  this->batch_write_times[tid] += timevalToDouble(e) - timevalToDouble(s);
  return FILEIO_SUCCESS;
}

//----------------------------------------------------------
// 0. First determine the starting address of the data block pointer (datapointer) to start the read from
// 1. Because the data items are variable sized, go through each of the next nItems metadata entries while summing up the lengths into totalLength
// 2. Read totalLength bytes of data starting at datapointer
template<typename T>
fileIoReturn_t TwoPhaseFileIO<T>::file_get_batch(const unsigned& tid, const IdType& startKey, unsigned nItems, T* arrayOfItems) {
  timeval s, e;
  gettimeofday(&s, NULL);
  //efprintf(stderr, "Key: %llu, nItems: %u\n", startKey, nItems);
  //unsigned tid = this->tid(startKey);
  unsigned hid = this->hash(startKey);
  this->batch_read_counts[tid]++;

  pthread_mutex_lock( &metaFileMutex[tid][hid] );
  // To save the metastart, first iteration is outside the loop
  off64_t metastart = lseek(meta_iofs[tid][hid], META_LINE_LENGTH * startKey, SEEK_SET); assert(metastart != -1);
  char length_str[META_LINE_LENGTH], *datapointer_str;
  int err = read(meta_iofs[tid][hid], length_str, META_LINE_LENGTH);
  if (err == -1){
  fprintf(stderr,"\nAssertion1 : %ld %i %lu %u %d Error: %s\n", length_str, meta_iofs[tid][hid], tid, hid, err, strerror(errno)); 
  }
  assert(err != -1);
  datapointer_str = length_str + MAX_CHARS_STREAMOFF + 1;
  length_str[MAX_CHARS_STREAMOFF] = datapointer_str[MAX_CHARS_STREAMOFF] = '\0';
  off64_t datapointer = atoll(datapointer_str);
  assert(datapointer != -1);

 // long* lengths = (long *)calloc(nItems, sizeof(long)); assert(lengths !=  NULL); //gk
  std::streamoff* lengths = (std::streamoff *)calloc(nItems, sizeof(std::streamoff)); assert(lengths !=  NULL); //gk
  lengths[0] = atol(length_str);
  for(unsigned i=startKey+1; i<startKey+nItems; i++) {
    err = read(meta_iofs[tid][hid], length_str, META_LINE_LENGTH);
  if(err == -1){ 
 fprintf(stderr,"\nAssertion2 : %s\n", strerror(errno)); 
}
    assert(err != -1);
    length_str[MAX_CHARS_STREAMOFF] = datapointer_str[MAX_CHARS_STREAMOFF] = '\0';
    lengths[i-startKey] = atol(length_str);
  }
  pthread_mutex_unlock( &metaFileMutex[tid][hid] );

  pthread_mutex_lock( &dataFileMutex[tid] );
  err = lseek(data_ifs[tid][hid], datapointer, SEEK_SET); assert(err != -1);
  for(unsigned i=startKey; i<startKey+nItems; i++) {
    void *buf = malloc(lengths[i-startKey]); assert(buf != NULL);
    err = read(data_ifs[tid][hid], buf, lengths[i-startKey]);
 if(err == -1){ 
 fprintf(stderr,"\nAssertion3 : %s \n", strerror(errno)); 
   }
   assert(err != -1);
 // fprintf(stderr,"\nafter assert3 i: %d, nItems: %d, startKey: %d, buf: %d, lengths: %d ",i, nItems, startKey,buf,lengths[i-startKey]); 
    bool good = arrayOfItems[i-startKey].ParseFromArray(buf, lengths[i-startKey]); 
//  fprintf(stderr,"\nAfter assertion 3 good : %d ", good); 
    assert(good = true);
    free(buf);
  }
 // fprintf(stderr,"\nunlock buffer : "); 
  pthread_mutex_unlock( &dataFileMutex[tid] );
  free(lengths);

  gettimeofday(&e, NULL);
  this->batch_read_times[tid] += timevalToDouble(e) - timevalToDouble(s);
  return FILEIO_SUCCESS;
}

