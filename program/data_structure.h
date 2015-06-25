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

struct Thread_data
{   
    int thread;
    Data * main_data_set;
    Data * foreign_data_set;
    int main_data_set_len;
    int foreign_data_set_len;
};

