//生成ZipfLow分布的数据集
// icc -lpthread genDataZipfLow.c -o genDataZipfLow
#define  LINUX
#undef NDEBUG
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <string.h>
#include <pthread.h>
#ifdef LINUX
#include <sys/time.h>
#else
#include <time.h>
#endif

#define  L1_LINE        64      //L1级cache一行的长度
#define  N              (1<<24) //zipf分布的数据变化范围
#define  FALSE          0
#define  TRUE           1
#define  THREAD_NUM     24
#define  STACKSIZE      10000000
pthread_attr_t attr;
typedef struct ARG
{
	int ID;
	int *data;
	int data_len;
}ARG;

//Zipf分布的参数（low）
//double alpha = 1.85;//高偏斜度关键字0超过一半的概率
double alpha = 1.05;//低偏斜度，三个数值总和加起来占三分一

//保存计算每个随机数后的值，每个线程一个，同时避免了乒乓现象
long xs[THREAD_NUM*L1_LINE/sizeof(long)];
//记录每个数出现的概率
double prob[N] = { 0 };
//待生成的数据长度
//int data_len[] = {1<<17, 1<<18,1<<19,1<<20,1<<21,1<<22, 1<<23 };
//int data_len[] = { 1<<24, 1<<25, 1<<26, 1<<27,1<<28,1<<29};

 unsigned int data_len[] = { 1<<17, 1<<18,1<<19,1<<20,1<<21,1<<22, 1<<23, 1<<24, 1<<25, 1<<26, 1<<27,1<<28,1<<29 };
//生成的文件名

 /*
char filenames[][256] = {												
							"DataZipfLow128K",
							"DataZipfLow256K",
							"DataZipfLow512K",
							"DataZipfLow1M",
							"DataZipfLow2M",
							"DataZipfLow4M",
							"DataZipfLow8M",
							
						};
*/				

char filenames[][256] = {
							"DataZipfLow128K",
							"DataZipfLow256K",
							"DataZipfLow512K",
							"DataZipfLow1M",
							"DataZipfLow2M",
							"DataZipfLow4M",
							"DataZipfLow8M",
							"DataZipfLow16M",
							"DataZipfLow32M",
							"DataZipfLow64M",
							"DataZipfLow128M",
							"DataZipfLow256M",
							"DataZipfLow512M"
													
						};
						

//线程函数
void* thread( void *param );
//计算各数出现的概率
void cal_prob( );
//返回(0,1.0)中的一个随机值
double rand_val( int ID, int seed );
//产生一个服从zipf分布的随机数
int zipf( int ID );

int main( void )
{
	ARG arg[THREAD_NUM];
	pthread_t tids[THREAD_NUM];
	FILE *fp;
	int *data;
	int i, j;
#ifdef LINUX
	struct timeval st, et;
#else
	clock_t st, et;
#endif

	//计算各数出现的概率
	cal_prob( );

	//生成各种数据量
	for ( i = 0; i < sizeof(data_len)/sizeof(int); i++ )
	{
#ifdef LINUX
		gettimeofday( &st, NULL );
#else
		st = clock( );
#endif
		printf( "data length: %d\n", data_len[i] );

		//创建存储数据的数组
		data = ( int* )malloc( sizeof(int)*data_len[i] );

		if(data==NULL)
		{printf("malloc error\n");
		exit(-1);
		}
		else
		{printf("mallic ok\n");
		}
		 size_t stacksize;
		//创建各线程
		for ( j = 0; j < THREAD_NUM; j++ )
		{
			arg[j].data = data;
			arg[j].data_len = data_len[i];
			arg[j].ID = j;
			pthread_attr_init(&attr);
			pthread_attr_setstacksize(&attr,STACKSIZE);
			pthread_attr_getstacksize(&attr,&stacksize);
			pthread_create( &tids[j], NULL, thread, &arg[j] );
		}
		for ( j = 0; j < THREAD_NUM; j++ )
		{
			pthread_join( tids[j], NULL );
		}

		//创建数据文件
		printf( "creating data file \"%s\" ...\n", filenames[i] );
		fp = fopen( filenames[i], "wb" );
		if ( fp == NULL )
		{
			printf( "fopen for \"%s\" error\n", filenames[i] );
			exit( 1 );
		}

		//将数据写入文件
		printf( "writing data file \"%s\" ...\n", filenames[i] );
		if ( fwrite(data,sizeof(int),data_len[i],fp) != data_len[i] )
		{
			printf( "fwrite error datalen=%d\n",data_len[i] );
			exit( 1 );
		}

		//释放数组
		free( data );

		fclose( fp );
#ifdef LINUX
		gettimeofday( &et, NULL );
		printf( "time used: %ldms\n", (et.tv_sec-st.tv_sec)*1000+(et.tv_usec-st.tv_usec)/1000 );
#else
		et = clock( );
		printf( "time used: %dms\n", (et-st)*1000/CLOCKS_PER_SEC );
#endif
		printf( "\n" );
	}

	printf( "done\n" );

	return 0;
}

void* thread( void *param )
{
	ARG *arg = ( ARG* )param;
	int ID = arg->ID;
	int *data = arg->data;
	int len = arg->data_len;
	long begin, end;
	int i;

	if ( len < THREAD_NUM )
	{
		begin = ( len>ID ) ? ID : len;
		end = ( len>(ID+1) ) ? ( ID+1 ) : len;
	}
	else
	{ long temp=len/THREAD_NUM;
		begin = ID * temp;
		end = ( ID + 1 ) * temp;
	}

	rand_val( ID, ID+1 );

	for ( i = begin; i < end; i++ )
	{
		data[i] = zipf( ID );
	}

	return NULL;
}

void cal_prob( )
{
	double put;
	double get;
	double c = 0;
	int i;

	for ( i = 1; i <= N; i++ )
		c = c + (1.0 / pow((double)i,alpha));
	for ( i = 1; i <= N; i++ )
		prob[i-1] = 1.0 / pow((double)i, alpha) / c;

	put = 0;
	for ( i = 0; i < N; i++ )
	{
		get = prob[i];
		prob[i] = put;
		put += get;
	}
}

//===========================================================================
//=  Function to generate Zipf (power law) distributed random variables     =
//=    - Input: alpha and N                                                 =
//=    - Output: Returns with Zipf distributed random variable              =
//===========================================================================
int zipf( int ID )
{
	double z;                     // Uniform random number (0 < z < 1)
	int    i;                     // Loop counter
	int low = 0;
	int high = N - 1;
	int mid;

	// Pull a uniform random number (0 < z < 1)
	z = rand_val( ID, 0 );

	while ( low <= high )
	{
		mid = ( low + high ) / 2;
		if ( z < prob[mid] ) { high = mid - 1; }
		else { low = mid + 1; }
	}

	// Assert that high is between 0 and N-1
	assert( (high>=0) && (high<N) );

	return high;
}

//=========================================================================
//= Multiplicative LCG for generating uniform(0.0, 1.0) random numbers    =
//=   - x_n = 7^5*x_(n-1)mod(2^31 - 1)                                    =
//=   - With x seeded to 1 the 10000th x value should be 1043618065       =
//=   - From R. Jain, "The Art of Computer Systems Performance Analysis," =
//=     John Wiley & Sons, 1991. (Page 443, Figure 26.2)                  =
//=   函数的周期为2147483646 = 2G，大于0，小于1                           =
//=========================================================================
double rand_val( int ID, int seed )
{
	const long  a =      16807;  // Multiplier
	const long  m = 2147483647;  // Modulus
	const long  q =     127773;  // m div a
	const long  r =       2836;  // m mod a
	long        x = xs[ID*L1_LINE/sizeof(long)];
	long        x_div_q;         // x divided by q
	long        x_mod_q;         // x modulo q
	long        x_new;           // New x value

	// Set the seed if argument is non-zero and then return zero
	if (seed > 0)
	{
		xs[ID*L1_LINE/sizeof(long)] = seed;
		return(0.0);
	}

	// RNG using integer arithmetic
	x_div_q = x / q;
	x_mod_q = x % q;
	x_new = (a * x_mod_q) - (r * x_div_q);
	if (x_new > 0)
		x = x_new;
	else
		x = x_new + m;

	xs[ID*L1_LINE/sizeof(long)] = x;
	// Return a random value between 0.0 and 1.0
	return((double) x / m);
}
