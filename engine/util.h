#ifndef __MR_UTIL_H_
#define __MR_UTIL_H_

#include <limits>
#include <iomanip>
#include <sstream>
#include <string>
#include <sys/time.h>
#include <tr1/random>
#include <vector>
#include <fstream>

#include <google/protobuf/stubs/common.h>
typedef ::google::protobuf::uint64 IdType;

#define MAX_ID_VALUE (ULONG_MAX)

#define ASSERT_WITH_MESSAGE(condition, message) do { \
  if (!(condition)) { printf((message)); } \
  assert ((condition)); } while(false)

extern std::tr1::mt19937 eng;

// enhanced fprintf using a variadic macro
#ifdef DEBUG
#define efprintf(ofile, fmt, ...) fprintf(stderr, "%s(): %d | "fmt, __func__, __LINE__, __VA_ARGS__);
//#define efprintf(ofile, fmt, ...) fprintf(stderr, "%s::%s(): %d | "fmt, __FILE__, __func__, __LINE__, __VA_ARGS__);
#else
#define efprintf(...) ;
#endif

char *itoa(int i, char *tmp);
unsigned numDigits(std::streamoff);
std::string zeroPadded(std::streamoff);
double timevalToDouble(timeval &t); 
double tmDiff(timeval &s, timeval &e);
double getTimer(); 
unsigned randomNumber(int floor, int ceiling);
IdType min(IdType, IdType);

void printCallStack();

void getListOfFiles(std::string directory, std::vector<std::string>* fileList);
void reduceFiles(const std::string& dir, std::vector<std::string>* fileList, uint32_t gb);
void printFileNames(const std::string& dir, std::vector<std::string>* fileList, uint32_t gb);
unsigned stringHash(const std::string& str);

  // Computing the digits using this method with ::max() gives 20 characters
  // Fetching the digits using ::digits gives 18. To be safe, sticking with ::max() method
#define MAX_CHARS_STREAMOFF ( numDigits(std::numeric_limits<std::streamoff>::max()) )

#endif //__MR_UTIL_H_

