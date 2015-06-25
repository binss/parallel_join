#include <sys/time.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <sys/time.h>
#include <vector>
#include <string>
#include "data_structure.h"
using std::string;


int thread_count;

// 显示用法
void usage(char* program_name) {

   fprintf(stderr, "usage: %s <join method> <foreign file index> <thread>\n", program_name);
   fprintf(stderr, "   join method = 1(nested loop join) 2(merge sort join)\n");
   fprintf(stderr, "   foreign file index = type * size/MB \n");
   exit(0);
}

void loadFile(const char * filename, Data * data_set, int data_set_len) 
{
    FILE *fpin;
    unsigned int* key_set = new UINT[data_set_len];   
    
    fpin = fopen( filename, "rb" );
    if ( fpin == NULL )
    {
        printf( "fopen for fpin error\n" );
        exit( 1 );
    }

    printf( "loading file %s\n", filename );
    if ( fread(key_set, sizeof(unsigned int), data_set_len, fpin) != data_set_len )
    {
        printf( "fread error\n" );
        exit( 1 );
    }
    for(UINT i=0; i<data_set_len; i++){
        data_set[i] = Data(i, key_set[i]);
    }
    delete[] key_set;
    fclose( fpin );
}



void saveResult(std::vector<MatchPair> pairs) 
{
    FILE *fout ;    
    fout = fopen( "../result/dudu.txt", "a+" );
    char *buffer = new char[20];
    for(int i=0; i<pairs.size(); i++){
        sprintf(buffer, "（%d, %d)\n", pairs[i].main_index, pairs[i].foreign_index);
        fputs(buffer, fout);

    }
    delete[] buffer;
    fclose( fout );
}



void * nestedLoopJoin(void *args){
    Thread_data *data = (Thread_data *) args;
    printf("Thread %d of %d start to work.\n", data->thread, thread_count);
    
    std::vector<MatchPair> pairs;
    for(int i=0; i < data->main_data_set_len; i++){
        for(int j=0; j < data->foreign_data_set_len; j++){
            if( data->main_data_set[i].key == data->foreign_data_set[j].key ){
                pairs.push_back(MatchPair(data->main_data_set[i].index, data->foreign_data_set[j].index));
            }
        }
    }
    printf("Thread %d of %d find %zu pairs\n", data->thread, thread_count, pairs.size());
    // saveResult(pairs);
    return NULL;
}

int compare(const void * a, const void * b)
{
    return (*(Data *)a).key - (*(Data *)b).key;
}

void *mergeSortJoin(void *args){    
    Thread_data *data = (Thread_data *) args;
    printf("Thread %d of %d start to work.\n", data->thread, thread_count);

    std::vector<MatchPair> pairs;

    qsort(data->main_data_set, data->main_data_set_len, sizeof(Data), compare);
    qsort(data->foreign_data_set, data->foreign_data_set_len, sizeof(Data), compare);

    int i = 0, j = 0;
    while( i < data->main_data_set_len && j < data->foreign_data_set_len){
        if( data->main_data_set[i].key == data->foreign_data_set[j].key ){
            pairs.push_back(MatchPair(data->main_data_set[i].index, data->foreign_data_set[j].index));
            // i++;
            j++;
        }
        else if( data->main_data_set[i].key < data->foreign_data_set[j].key){
            i++;
        }
        else{
            j++;
        }
    }
    printf("Thread %d of %d find %zu pairs\n", data->thread, thread_count, pairs.size());
    // saveResult(pairs);
    return NULL;
}

int main(int argc, char * argv[]){
    if(argc != 4)
        usage(argv[0]);

    int method = atoi(argv[1]);
    int index = atoi(argv[2]);

    string data_path = string("../DataSet/");
    string main_file_name = string("DataKey128K");
    string foreign_file_names[] = {"DataUniform1M", "DataUniform2M", "DataUniform4M", "DataUniform8M",
                                   "DataZipfHigh1M", "DataZipfHigh2M", "DataZipfHigh4M", "DataZipfHigh8M", 
                                   "DataZipfLow1M",  "DataZipfLow2M", "DataZipfLow4M", "DataZipfLow8M",  
                                  };
    int main_data_set_len = 1<<17;
    int foreign_data_set_len = 1<<(20 + index % 4);
    string main_file_path = data_path + main_file_name;
    string foreign_file_path = data_path + foreign_file_names[index];

    Data * main_data_set = new Data[main_data_set_len];
    loadFile(main_file_path.c_str(), main_data_set, main_data_set_len);

    Data * foreign_data_set = new Data[foreign_data_set_len];
    loadFile(foreign_file_path.c_str(), foreign_data_set, foreign_data_set_len);

    thread_count = atoi(argv[3]);
    int thread;
    pthread_t * thread_handles = new pthread_t[thread_count];
    int part_length = foreign_data_set_len / thread_count;

    struct timeval start, end;
    gettimeofday(&start, NULL);


    for(thread=0; thread < thread_count; thread++){
        struct Thread_data *data = new Thread_data();
        data->main_data_set = main_data_set;
        data->main_data_set_len = main_data_set_len;
        data->foreign_data_set = foreign_data_set + thread * part_length;
        data->foreign_data_set_len = part_length;
        data->thread = thread;

        if(method == 1)
            pthread_create(&thread_handles[thread], NULL, nestedLoopJoin, (void *)data);
        else if(method == 2)
            pthread_create(&thread_handles[thread], NULL, mergeSortJoin, (void *)data);
        else{
            fprintf(stderr, "method error \n");
            exit(0);
        }
    }    

    for(thread=0; thread < thread_count; thread++){
        pthread_join(thread_handles[thread], NULL);
    }

    gettimeofday(&end, NULL);
    long interval = 1000000 * ( end.tv_sec - start.tv_sec ) + end.tv_usec - start.tv_usec;  
    printf("Total times: %ld microseconds\n", interval);

    delete[] thread_handles;
}