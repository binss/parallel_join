//by sheldont 20150317
#include "common.h"

//index代表第几个数据文件文件，key[]存放主键,keylen表示数组key的长度
void loaddata(char filenames[][NAMEMAX],int index, int* data_len,unsigned int* key,int *keylen) 
{

	FILE *fpin ;	
	fpin = fopen( filenames[index], "rb" );
	if ( fpin == NULL )
	{
		printf( "fopen for fpin error\n" );
		exit( 1 );
	}

	printf( "loading file %s\n", filenames[index] );
	if ( (*keylen=fread(key,sizeof(int),data_len[index],fpin)) != data_len[index] )
	{
		printf( "fread error\n" );
		exit( 1 );
	}
	fclose( fpin );
}

