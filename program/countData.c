#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#define NAMEMAX 256	//文件名最大长度
//统计每个数据集中各个数字出现的频率
//编译：icc -lpthread countData.c -o countData
char filenames[][NAMEMAX] = {
	/*
    							"DataZipfLow128K",
								"DataZipfLow256K",
								"DataZipfLow512K",
								"DataZipfLow1M",
								"DataZipfLow2M",
								"DataZipfLow4M",
								"DataZipfLow8M"
*/
	
	                    
							//"DataZipfHigh16M","DataZipfHigh32M","DataZipfHigh64M","DataZipfHigh128M","DataZipfHigh256M","DataZipfHigh512M",
						    "DataZipfLow16M","DataZipfLow32M","DataZipfLow64M","DataZipfLow128M","DataZipfLow256M","DataZipfLow512M"

							};
int data_len[] = {
				    1<<24, 1<<25, 1<<26, 1<<27,1<<28,1<<29
//					1<<24, 1<<25, 1<<26, 1<<27, 1<<28,1<<29
//					1<<17, 1<<18,1<<19,1<<20,1<<21,1<<22, 1<<23, 1<<24, 1<<25, 1<<26, 1<<27,1<<28,1<<29
				};	
/*				
char filenames[][NAMEMAX] = {"DataKey0M16M"		};	
int data_len[] = {1<<24		};						
*/						


//统计最终产生的数据
void get_stats( void );

int main( void )
{
	printf( "counting ...\n" );
	get_stats( );
	printf( "done\n" );

	return 0;
}

void get_stats( void )
{
	int H[256];
	char outname[256];
	FILE *fpin, *fpout;
	unsigned char *p_begin, *p_end;
	unsigned char *p;
	int *arr1, *arr2;
	int n;
	int temp;
	int i, j, k;

	//统计每一种数据量的信息
	//将"DataUniform_xxx.txt"中的信息统计到"DataUniform_xxx_stats.txt" 其实就是统计每个数字出现的次数，DataUniform_xxx_stats.txt是直方图
	for ( i = 0; i < sizeof(filenames)/(sizeof(char)*NAMEMAX); i++ )
	{
		//arr1数组用于存储从数据文件中读出的数据
		arr1 = ( int* )malloc( sizeof(int)*data_len[i] );
		if ( arr1 == NULL )
		{
			printf( "malloc for arr1 error\n" );
			exit( 1 );
		}
		//arr2数组用于排序时辅助
		arr2 = ( int* )malloc( sizeof(int)*data_len[i] );
		if ( arr2 == NULL )
		{
			printf( "malloc for arr2 error\n" );
			exit( 1 );
		}

		//打开数据文件"DataUniform_xxx.txt"
		printf( "opening file %s\n", filenames[i] );
		fpin = fopen( filenames[i], "rb" );
		if ( fpin == NULL )
		{
			printf( "fopen for fpin error\n" );
			//exit( 1 );
			continue;
		}

		//将数据文件中的数据读出到数组arr1中
		printf( "reading file %s\n", filenames[i] );
		if ( (n=fread(arr1,sizeof(int),data_len[i],fpin)) != data_len[i] )
		{
			printf( "fread error\n" );
			exit( 1 );
		}

		//对arr1中的各数进行排序，方便统计各个数出现的次数，用基数排序的方法
		printf( "counting for file %s\n", filenames[i] );
		//第一轮
		//初始化直方图
		memset( H, 0, sizeof(int)*256 );
		//统计直方图
		p_begin = ( unsigned char* )&arr1[0];
		p_end = ( unsigned char* )&arr1[n];
		for ( p = p_begin; p < p_end; p+=4 )
		{
			H[*p]++;
		}
		//更新直方图
		temp = n;
		for ( j = 255; j >= 0; j-- )
		{
			temp = H[j] = temp - H[j];
		}
		//将数据写入arr2
		for ( p = p_begin; p < p_end; p+=4 )
		{
			arr2[H[*p]++] = *( int* )p;
		}
		//第二轮
		//初始化直方图
		memset( H, 0, sizeof(int)*256 );
		//统计直方图
		p_begin = ( unsigned char* )&arr2[0];
		p_end = ( unsigned char* )&arr2[n];
		for ( p = p_begin+1; p < p_end; p+=4 )
		{
			H[*p]++;
		}
		//更新直方图
		temp = n;
		for ( j = 255; j >= 0; j-- )
		{
			temp = H[j] = temp - H[j];
		}
		//将数据写入arr1
		for ( p = p_begin+1; p < p_end; p+=4 )
		{
			arr1[H[*p]++] = *( int* )( p-1 );
		}
		//第三轮
		//初始化直方图
		memset( H, 0, sizeof(int)*256 );
		//统计直方图
		p_begin = ( unsigned char* )&arr1[0];
		p_end = ( unsigned char* )&arr1[n];
		for ( p = p_begin+2; p < p_end; p+=4 )
		{
			H[*p]++;
		}
		//更新直方图
		temp = n;
		for ( j = 255; j >= 0; j-- )
		{
			temp = H[j] = temp - H[j];
		}
		//将数据写入arr2
		for ( p = p_begin+2; p < p_end; p+=4 )
		{
			arr2[H[*p]++] = *( int* )( p-2 );
		}
		//第四轮
		//初始化直方图
		memset( H, 0, sizeof(int)*256 );
		//统计直方图
		p_begin = ( unsigned char* )&arr2[0];
		p_end = ( unsigned char* )&arr2[n];
		for ( p = p_begin+3; p < p_end; p+=4 )
		{
			H[*p]++;
		}
		//更新直方图
		temp = n;
		for ( j = 127; j >= 0; j-- )
		{
			temp = H[j] = temp - H[j];
		}
		for ( j = 255; j > 127; j-- )
		{
			temp = H[j] = temp - H[j];
		}
		//将数据写入arr1
		for ( p = p_begin+3; p < p_end; p+=4 )
		{
			arr1[H[*p]++] = *( int* )( p-3 );
		}

		//创建输出文件
		strcpy( outname, filenames[i] );
		strcat( outname, "_stats" );
		printf( "creating file %s\n", outname );
		fpout = fopen( outname, "w" );
		if ( fpout == NULL )
		{
			printf( "fopen for \"%s\" error\n", outname );
			exit( 1 );
		}
		//将数据写到输出文件
		printf( "writing file %s\n", outname );
		fprintf( fpout, "number      times       ratio(total times=%d)\n", n );
		
		int tempsum = 0 ;
		
		for ( j = 0; j < n; j = k )
		{
			//寻找相等的数据，它起始于j，终止于k
			k = j + 1;
			while ( arr1[k] == arr1[j] )
			{
				k++;
			}

			tempsum +=( k-j)*(k-j);
			//这些数据的个数为(k-j)个
			fprintf( fpout, "%-10d  %-10d  %f\n", arr1[j], k-j, (double)(k-j)/n );
		}
		printf( "\n" );
        printf("xieluhui tempsum=%d\n",tempsum);
		fclose( fpout );
		fclose( fpin );
		free( arr1 );
		free( arr2 );
	}
}
