#include<stdio.h>
#include<unistd.h>
#include<sys/stat.h>
#include<sys/types.h>
#include<fcntl.h>
#include<unistd.h>

int main(int argc,char*argv[])
{
    int fd = ::open("/tmp/testdb/test.ldb",O_RDONLY,0644);
    char t;
    if(lseek(fd, 1,SEEK_SET) == -1)
    {
        printf("error!\n");
    }
    while(::read(fd,&t,1) > 0)
    {
        printf(" %d ",t);
    }
    printf("\n");

    return 0;
}