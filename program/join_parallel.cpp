#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <sys/time.h>
#include <vector>

typedef unsigned int UINT;


int thread_count = 2;


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

struct MatchPair{
    int main_index;
    int foreign_index;
    MatchPair(int x,int y){
        main_index = x;
        foreign_index = y;
    }
};

struct Thread_data
{   
    int thread;
    UINT * main_data_set;
    UINT * foreign_data_set;
    int main_data_set_len;
    int foreign_data_set_len;
};

struct Row{
    UINT data;
    UINT index;
};

void saveResult(std::vector<MatchPair> pairs) 
{
    FILE *fout ;    
    fout = fopen( "../result/dudu.txt", "w" );
    char *buffer = new char[20];
    for(int i=0; i<pairs.size(); i++){
        sprintf(buffer, "（%d, %d)\n", pairs[i].main_index, pairs[i].foreign_index);
        fputs(buffer, fout);

    }
    delete[] buffer;
    fclose( fout );
}



void * nestedLoopJoin(void *rank){
    Thread_data *data = (Thread_data *) rank;
    printf("Thread %d of %d start to work.\n", data->thread, thread_count);
    std::vector<MatchPair> pairs;
    for(int i=0; i < data->main_data_set_len; i++){
        for(int j=0; j < data->foreign_data_set_len; j++){
            if( data->main_data_set[i] == data->foreign_data_set[j] ){
                pairs.push_back(MatchPair(i, j));
            }
        }
    }
    printf("%lu\n", pairs.size());
    return NULL;
}



int main(int argc, char * argv[]){
    char main_file_name[] = "../DataSet/DataKey128K";
    int main_data_set_len = 1<<17;
    UINT * main_data_set = new UINT [main_data_set_len];
    loadFile(main_file_name, main_data_set, main_data_set_len);

    char foreign_file_name[] = "../DataSet/DataUniform1M";
    int foreign_data_set_len = 1<<20;
    UINT * foreign_data_set = new UINT [foreign_data_set_len];
    loadFile(foreign_file_name, foreign_data_set, foreign_data_set_len);

    // mergeSortJoin(main_data_set, main_data_set_len, foreign_data_set, foreign_data_set_len);
    int thread;
    pthread_t * thread_handles;
    thread_handles = (pthread_t *) malloc(thread_count * sizeof(pthread_t));

    int part_length = main_data_set_len / thread_count;


    for(thread=0; thread < thread_count; thread++){
        struct Thread_data *data = new Thread_data();
        data->main_data_set = main_data_set + thread * part_length;
        data->main_data_set_len = part_length;
        data->foreign_data_set = foreign_data_set;
        data->foreign_data_set_len = foreign_data_set_len;
        data->thread = thread;
        pthread_create(&thread_handles[thread], NULL, nestedLoopJoin, (void *)data);
    }    

    for(thread=0; thread < thread_count; thread++){
        pthread_join(thread_handles[thread], NULL);
    }
    free(thread_handles);
    // printf("%lu\n", pairs.size());
}