#include"leveldb/db.h"
#include "db/skiplist.h"
#include "util/arena.h"
#include <iostream>
#include <string.h>
#include <stdlib.h>

typedef uint64_t Key;

struct Comparator {
  int operator()(const Key& a, const Key& b) const {
    if (a < b) {
      return -1;
    } else if (a > b) {
      return +1;
    } else {
      return 0;
    }
  }
};

int main(int argc,char* argv[])
{

    {
        leveldb::Arena arena;
        Comparator cmp;
        leveldb::SkipList<Key, Comparator> list(cmp, &arena);
        // leveldb::SkipList<Key, Comparator>::Iterator iter(&list);
        list.Insert(1);

    }
    // {
    //     leveldb::DB *db;
    //     leveldb::Options options;
    //     leveldb::Status status;

    //     std::string key1 = "key1";
    //     std::string val1 = "val1", val;

    //     options.create_if_missing = true;

    //     status = leveldb::DB::Open(options, "/tmp/testdb", &db);

    //     assert(status.ok());
    //     std::cout << status.ToString() << std::endl;
    

    //     status = db->Put(leveldb::WriteOptions(), key1, val1);
    //     assert(status.ok());
    // }
    return 0;
}