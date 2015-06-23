#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <sys/time.h>
#include <vector>

typedef unsigned int UINT;

struct Data{
    UINT index;
    UINT key;

    Data(UINT r=0, UINT k=0){
        index = r;
        key = k;
    }
};

struct MatchPair{
    UINT main_index;
    UINT foreign_index;
    MatchPair(UINT x, UINT y){
        main_index = x;
        foreign_index = y;
    }
};

void loadFile(char * filename, Data * data_set, int data_set_len) 
{
    FILE *fpin ;    
    fpin = fopen( filename, "rb" );
    if ( fpin == NULL )
    {
        printf( "fopen for fpin error\n" );
        exit( 1 );
    }

    printf( "loading file %s\n", filename );
    unsigned int* key_set = new UINT [data_set_len];
    if ( fread(key_set, sizeof(unsigned int), data_set_len, fpin) != data_set_len )
    {
        printf( "fread error\n" );
        exit( 1 );
    }
    for(UINT i=0; i<data_set_len; i++){
        // struct Data data = { index = i, key = key_set[i]};
        data_set[i] = Data(i, key_set[i]);
    }

    fclose( fpin );
}



void saveResult(std::vector<MatchPair> pairs) 
{
    FILE *fout ;    
    fout = fopen( "../result/dudu3.txt", "w" );
    char *buffer = new char[20];
    for(int i=0; i<pairs.size(); i++){
        sprintf(buffer, "（%d, %d)\n", pairs[i].main_index, pairs[i].foreign_index);
        fputs(buffer, fout);

    }
    delete[] buffer;
    fclose( fout );
}


void nestedLoopJoin(Data * main_data_set, int main_data_set_len, Data * foreign_data_set, int foreign_data_set_len){
    std::vector<MatchPair> pairs;

    for(int i=0; i < main_data_set_len; i++){
        for(int j=0; j < foreign_data_set_len; j++){
            if( main_data_set[i].key == foreign_data_set[j].key ){
                pairs.push_back(MatchPair(main_data_set[i].index, foreign_data_set[j].index));
            }
        }
    }
    printf("%lu\n", pairs.size());
    saveResult(pairs);
}

int compare(const void * a, const void * b)
{
    return (*(Data *)a).key - (*(Data *)b).key;
}

void mergeSortJoin(Data * main_data_set, int main_data_set_len, Data * foreign_data_set, int foreign_data_set_len){
    std::vector<MatchPair> pairs;
    qsort(main_data_set, main_data_set_len, sizeof(Data), compare);
    qsort(foreign_data_set, foreign_data_set_len, sizeof(Data), compare);
    int i = 0, j=0;
    // while( i < main_data_set_len && j < foreign_data_set_len){
    //     if( main_data_set[i].key == foreign_data_set[j].key ){
    //         pairs.push_back(MatchPair(main_data_set[i].index, foreign_data_set[j].index));
    //         i++;
    //         j++;
    //     }
    //     else if( main_data_set[i].key < foreign_data_set[j].key){
    //         i++;
    //     }
    //     else{
    //         j++;
    //     }
    // }
    // for(int i=0; i < main_data_set_len; i++){
    //     for(int j=0; j < foreign_data_set_len; j++){
    //         if( main_data_set[i].key < foreign_data_set[j].key){
    //             break;
    //         }
    //         if( main_data_set[i].key == foreign_data_set[j].key ){
    //             pairs.push_back(MatchPair(main_data_set[i].index, foreign_data_set[j].index));
    //         }
    //     }
    // }
    printf("%lu\n", pairs.size());
    saveResult(pairs);
}


int main(int argc, char * argv[]){
    char main_file_name[] = "../DataSet/DataKey128K";
    int main_data_set_len = 1<<17;
    Data * main_data_set = new Data[main_data_set_len];
    loadFile(main_file_name, main_data_set, main_data_set_len);

    char foreign_file_name[] = "../DataSet/DataUniform1M";
    int foreign_data_set_len = 1<<20;
    Data * foreign_data_set = new Data[foreign_data_set_len];
    loadFile(foreign_file_name, foreign_data_set, foreign_data_set_len);

    // nestedLoopJoin(main_data_set, main_data_set_len, foreign_data_set, foreign_data_set_len);
    // mergeSortJoin(main_data_set, main_data_set_len, foreign_data_set, foreign_data_set_len);
    

}