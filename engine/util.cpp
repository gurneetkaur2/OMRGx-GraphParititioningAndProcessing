
#include "util.h"

#include <iostream>
#include <vector>
#include <boost/algorithm/string.hpp>
#include <cassert>
#include <execinfo.h> // for backtrace
#include <stdlib.h>
#include <ctype.h>
#include <dirent.h>

std::tr1::mt19937 eng;

//------------------------------------------------------------------
char *itoa(int i, char *tmp) {
  sprintf(tmp, "%d", i);
  return tmp;
}

//------------------------------------------------------------------
// Implicitly assumes unsigned numbers; does not count any leading sign
unsigned numDigits(std::streamoff num) {
  int ndigits = 0;
  while (num) {
    num /= 10;
    ndigits++;
  }
  return ndigits;
}

//------------------------------------------------------------------
std::string zeroPadded(std::streamoff num) {
  std::ostringstream ss;
  ss << std::setw( MAX_CHARS_STREAMOFF ) << std::setfill( '0' ) << num;
  return ss.str();
}

//------------------------------------------------------------------
double tmDiff(timeval &s, timeval&e) {
  return timevalToDouble(e) - timevalToDouble(s);
}

//------------------------------------------------------------------
double timevalToDouble(timeval &t) {
  return t.tv_sec*1000.0 + t.tv_usec/1000.0;
}

//------------------------------------------------------------------
double getTimer() {
  struct timeval t;
  gettimeofday(&t, NULL);
  return t.tv_sec*1000 + t.tv_usec/1000.0;
}

//------------------------------------------------------------------
unsigned randomNumber(int floor, int ceiling) {
  std::tr1::uniform_int<int> unif(floor, ceiling-1);
  return unif(eng);
}

//------------------------------------------------------------------
IdType min(IdType a, IdType b) {
  return a<b ? a : b;
}

//------------------------------------------------------------------
// http://www.gnu.org/software/libc/manual/html_node/Backtraces.html
// To see function names, on command line pass OPTS+=-rdynamic
void printCallStack() {
  const unsigned CALL_STACK_SIZE = 50;
  void *array[CALL_STACK_SIZE];
  char **strings;

  size_t size = backtrace(array, CALL_STACK_SIZE);
  strings = backtrace_symbols(array, size);

  for(size_t i = 0; i < size; i++) {
    printf("%s\n", strings[i]);

    // Uncomment these and replace sssp with the name of this binary
    // char syscom[256];
    // sprintf(syscom,"addr2line %p -e sssp", array[i]); //last parameter is the name of this app
    // system(syscom);
  }
  printf("-----------------------\n");
  free(strings);
}

void getListOfFiles(std::string directory, std::vector<std::string>* fileList)
{
  DIR *dp;
  struct dirent *dirp;

  if ((dp = opendir(directory.c_str())) == NULL)
    return;

  //Also check . and .. are not included in the filename
  std::string forbiddenFilename1 = ".";
  std::string forbiddenFilename2 = "..";
  while ((dirp = readdir(dp)) != NULL)
  {
    std::string fname = std::string(dirp->d_name);
    if (fname.compare(forbiddenFilename1) != 0 && fname.compare(forbiddenFilename2) != 0)
    {
      fileList->push_back(fname);
    }
  }
  closedir(dp);
}

void reduceFiles(const std::string& dir, std::vector<std::string>* fileList, uint32_t gb) {
  uint64_t mul = 1024;
  uint64_t bytes = gb * mul * mul * mul;
  uint64_t total = 0; 
  unsigned i=0;
  for(i=0; i<fileList->size(); ++i) {
    std::string filename = dir; filename += '/'; filename += fileList->at(i);
    std::ifstream in(filename.c_str(), std::ifstream::ate | std::ifstream::binary);
    total += uint64_t(in.tellg());
    if(total > bytes)
      break;
  }
  if(i < fileList->size())
    fileList->resize(i + 1);
}

void printFileNames(const std::string& dir, std::vector<std::string>* fileList, uint32_t gb) {
  std::string fname = "input_"; fname += std::to_string(gb); fname += "gb.list";
  std::ofstream out(fname.c_str());
  for(unsigned i=0; i<fileList->size(); ++i) {
    out << dir << '/' << fileList->at(i) << std::endl;
  }
  out.close();
}

unsigned stringHash(const std::string& str) {
  //return static_cast<unsigned>(tolower(str[0]) - 'a');
  unsigned ret = 0;
  for(unsigned i=0; i<str.size(); ++i)
    ret += str[i];

  return ret;
}
