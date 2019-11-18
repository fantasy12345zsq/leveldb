#include"leveldb/db.h"
#include <iostream>
#include <stdlib.h>
#include <vector>

int main()
{
    leveldb::DB *db;
    leveldb::Options options;
    leveldb::Status status;

    std::vector<std::string> val = {"fre1","miuku7","hytju","jujt","ky65u","myum","y56jyuj","kewtg","jkuik","u76u7","kiukl","qwee","grfh","hyjng","ytjt","trhtg",
                                     "vgrt1","qt5h","hrtjhy","juju","wqtqerg",",ikuu","vjuyk","vghjj","re6y","j67jk","tert","sffsd","fgreg","sfew","wds","dwefws"};
    std::vector<std::string> key = {"vgrt1","qt5h","hrtjhy","juju","wqtqerg",",ikuu","vjuyk","vghjj","re6y","j67jk","tert","sffsd","fgreg","sfew","wds","dwefws",
                                    "fre1","miuku7","hytju","jujt","ky65u","myum","y56jyuj","kewtg","jkuik","u76u7","kiukl","qwee","grfh","hyjng","ytjt","trhtg"};

    // std::vector<std::string> val = {"fre1","miuku7","hytju","jujt","ky65u","myum","y56jyuj","kewtg","jkuik","u76u7","kiukl","qwee","grfh","hyjng","ytjt","trhtg"};
    // std::vector<std::string> key = {"vgrt1","qt5h","hrtjhy","juju","wqtqerg",",ikuu","vjuyk","vghjj","re6y","j67jk","tert","sffsd","fgreg","sfew","wds","dwefws"};
    options.create_if_missing = true;
    options.write_buffer_size = 256;
    options.block_size = 16;
    // options.max_open_files = 2;
    

    status = leveldb::DB::Open(options, "/tmp/testdb", &db);

    printf("after DB::open()!\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n");

    if (!status.ok())
    {
        std::cout << status.ToString() << std::endl;
        exit(1);
    }

    for(int i = 0; i < 32; i++)
    {
        
        status = db->Put(leveldb::WriteOptions(),key[i],val[i]);
        if(!status.ok())
        {
            std::cout << status.ToString() << std::endl;
            exit(2);
        }
    }


     

    // std::string key1("dwefws");
    // std::string val1;
    // status = db->Get(leveldb::ReadOptions(), key1, &val1);
    // if (!status.ok())
    // {

    //     std::cout << status.ToString() << std::endl;
    //     exit(2);
    // }
    // std::cout << "val = " << val1.data() << std::endl;

    
    while(1)
    {
        
    }



    // status = db->Get(leveldb::ReadOptions(), key1, &val);
    // if (!status.ok())
    // {
    //     std::cout << status.ToString() << std::endl;
    //     exit(3);
    // }

    // std::cout << "Get val: " << val << std::endl;
    // status = db->Delete(leveldb::WriteOptions(), key1);

    // if (!status.ok())
    // {
    //     std::cout << status.ToString() << std::endl;
    //     exit(4);
    // }


    // status = db->Get(leveldb::ReadOptions(), key1, &val);
    // if (!status.ok())
    // {

    //     std::cout << status.ToString() << std::endl;
    //     exit(5);

    // }

    // std::cout << "Get val: " << val << std::endl;
    return 0;

}
