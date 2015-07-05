#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <vector>
#include <string>
#include "data_structure.h"
using std::string;


// 显示用法
void usage(char* program_name){

   fprintf(stderr, "usage: %s <join method> <foreign file index>\n", program_name);
   fprintf(stderr, "   join method = 1(nested loop join) 2(merge sort join)\n");
   fprintf(stderr, "   foreign file index = type * size/MB \n");
   exit(0);
}

// 读取数据集并格式化
void loadFile(const char * filename, Data * data_set, int data_set_len){
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


// 将匹配结果写入文件
void saveResult(std::vector<MatchPair> pairs){
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

// 嵌套循环
void nestedLoopJoin(Data * main_data_set, int main_data_set_len, Data * foreign_data_set, int foreign_data_set_len){
    std::vector<MatchPair> pairs;

    for(int i=0; i < main_data_set_len; i++){
        for(int j=0; j < foreign_data_set_len; j++){
            if( main_data_set[i].key == foreign_data_set[j].key ){
                pairs.push_back(MatchPair(main_data_set[i].index, foreign_data_set[j].index));
            }
        }
    }
    printf("find %zu pairs\n", pairs.size());
    // saveResult(pairs);
}

int compare(const void * a, const void * b){
    return (*(Data *)a).key - (*(Data *)b).key;
}

// 排序合并
void mergeSortJoin(Data * main_data_set, int main_data_set_len, Data * foreign_data_set, int foreign_data_set_len){
    std::vector<MatchPair> pairs;
    qsort(foreign_data_set, foreign_data_set_len, sizeof(Data), compare);
    int i = 0, j = 0;
    while( i < main_data_set_len && j < foreign_data_set_len){
        if( main_data_set[i].key == foreign_data_set[j].key ){
            pairs.push_back(MatchPair(main_data_set[i].index, foreign_data_set[j].index));
            if( i == main_data_set_len - 1 || j == foreign_data_set_len - 1)
                i++;
            else if(main_data_set[i+1].key > main_data_set[j+1].key)
                j++;
            else
                i++;
        }
        else if( main_data_set[i].key < foreign_data_set[j].key){
            i++;
        }
        else{
            j++;
        }
    }

    printf("find %zu pairs\n", pairs.size());
    // saveResult(pairs);
}


int main(int argc, char * argv[]){
    if(argc != 3)
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

    struct timeval start, end;
    gettimeofday(&start, NULL);
    if(method == 1){
        nestedLoopJoin(main_data_set, main_data_set_len, foreign_data_set, foreign_data_set_len);
    }
    else if(method == 2){
        qsort(main_data_set, main_data_set_len, sizeof(Data), compare);
        mergeSortJoin(main_data_set, main_data_set_len, foreign_data_set, foreign_data_set_len);
    }
    else{
        fprintf(stderr, "method error \n");
        exit(0);
    }
    
    gettimeofday(&end, NULL);
    long interval = 1000000 * ( end.tv_sec - start.tv_sec ) + end.tv_usec - start.tv_usec;  
    printf("Total times: %ld microseconds\n", interval);
}