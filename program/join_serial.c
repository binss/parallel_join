#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <sys/time.h>

// char filenames[][NAMEMAX] = {
//                                 "DataKey0M16M",
//                              // "DataUniform16M","DataUniform32M","DataUniform64M","DataUniform128M","DataUniform256M","DataUniform512M","DataUniform1024M",
//                               //  "DataZipfHigh16M","DataZipfHigh32M","DataZipfHigh64M","DataZipfHigh128M","DataZipfHigh256M","DataZipfHigh512M",
//                                 // "DataZipfLow16M","DataZipfLow32M","DataZipfLow64M","DataZipfLow128M","DataZipfLow256M","DataZipfLow512M"
//                             };


// int data_len[] = {
//                     1<<24,
//                     1<<24, 1<<25, 1<<26, 1<<27,1<<28,1<<29,1<<30,
//                     1<<24, 1<<25, 1<<26, 1<<27,1<<28,1<<29,
//                     1<<24, 1<<25, 1<<26, 1<<27,1<<28,1<<29
//                 };

//index代表第几个数据文件文件，key[]存放主键,keylen表示数组key的长度
// void loaddata(char filenames[][NAMEMAX],int index, int* data_len,unsigned int* key,int *keylen) 
// {

//     FILE *fpin ;    
//     fpin = fopen( filenames[index], "rb" );
//     if ( fpin == NULL )
//     {
//         printf( "fopen for fpin error\n" );
//         exit( 1 );
//     }

//     printf( "loading file %s\n", filenames[index] );
//     if ( (*keylen=fread(key,sizeof(int),data_len[index],fpin)) != data_len[index] )
//     {
//         printf( "fread error\n" );
//         exit( 1 );
//     }
//     fclose( fpin );
// }

//index代表第几个数据文件文件，key[]存放主键,keylen表示数组key的长度

void loadFile(char * filename, unsigned int* data_set, int data_set_len) 
{
    FILE *fpin ;    
    fpin = fopen( filename, "rb" );
    if ( fpin == NULL )
    {
        printf( "fopen for fpin error\n" );
        exit( 1 );
    }

    printf( "loading file %s\n", filename );
    if ( fread(data_set, sizeof(unsigned int), data_set_len, fpin) != data_set_len )
    {
        printf( "fread error\n" );
        exit( 1 );
    }
    fclose( fpin );
}

int main(int argc, char * argv[]){
    char *filename = "../DataSet/DataKey128K";
    int data_set_len = 1<<17;
    unsigned int* data_set = malloc(data_set_len * sizeof(unsigned int));
    loadFile(filename, data_set, data_set_len);
    int x = 0;
    for(int i=0; i < data_set_len; i++)
        for(int j=0; j < data_set_len; j++){
            x += data_set[i] + data_set[j];
        }
    // printf("%d\n", data_set[200]);
}