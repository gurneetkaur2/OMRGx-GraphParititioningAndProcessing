#ifndef __RECORD_TYPE__
#define __RECORD_TYPE__

#include <string>
#include <cassert>

struct RecordType {
  public:

    const std::string key() const { 
      return _key;
    }

    void set_key(const std::string& k) {
      assert(k.size() <= MAX_WORD_SIZE);
      std::copy(k.begin(), k.end(), _key);
      _key[k.size()] = '\0';
    } 

    const uint64_t value() const {
      return _value;
    }

    void set_value(const uint64_t& v) {
      _value = v;
    }

  private:
    char _key[MAX_WORD_SIZE + 1];
    uint64_t _value;
};

#endif
