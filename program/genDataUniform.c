//生成均匀分布的数据集
// icc -lpthread genDataUniform.c -o genDataUniform
#define  LINUX
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#ifdef LINUX
#include <sys/time.h>
#else
#include <time.h>
#endif

#define  L1_LINE        64
#define  THREAD_NUM     8
#define  STACKSIZE      10000000
#define  boundry_a      0           //取值的下边界，可以取到
#define  boundry_b      (1<<24)     //取值的上边界，取不到
pthread_attr_t attr;

typedef struct ARG
{
	int ID;
	int *data;
	int data_len;
}ARG;

//保存计算每个随机数后的值，每个线程一个，同时避免了乒乓现象
long xs[THREAD_NUM*L1_LINE/sizeof(long)];
//待生成的数据长度
//unsigned int data_len[] = { 1<<22, 1<<23, 1<<24, 1<<25, 1<<26, 1<<27,1<<28,1<<29,1<<30 };
unsigned int data_len[] = { 1<<17, 1<<18,1<<19,1<<20,1<<21,1<<22, 1<<23 };
//生成的文件名
/*char filenames[][256] = {	"DataUniform4M",
							"DataUniform8M",
							"DataUniform16M",
							"DataUniform32M",
							"DataUniform64M",
							"DataUniform128M",
							"DataUniform256M",
							"DataUniform512M",
							"DataUniform1024M",
						};
*/
char filenames[][256] = {
							"DataUniform128K",
							"DataUniform256K",
							"DataUniform512K",
							"DataUniform1M",
							"DataUniform2M",
							"DataUniform4M",
							"DataUniform8M"
						};

//返回(0,1.0)中的一个随机值
double rand_val( int ID, int seed );
//线程函数
void* thread( void *param );

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

	//生成各种数据量
	for ( i = 0; i < sizeof(data_len)/sizeof(int); i++ )
//	for ( i = 2; i < 3; i++ )
	{
#ifdef LINUX
		gettimeofday( &st, NULL );
#else
		st = clock( );
#endif
		printf( "data length: %d\n", data_len[i] );

		//创建存储数据的数组
		data = ( int* )malloc( sizeof(int)*data_len[i] );
		if ( data == NULL )
		{
			printf( "malloc error\n" );
			exit( 1 );
		}
       else
	   { 
			printf( "malloc ok\n" );
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
		//	printf("the stack size=%ld bytes\n",stacksize);
			pthread_create( &tids[j], &attr, thread, &arg[j] );
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
			printf( "fwrite error\n" );
			exit( 1 );
		}

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
   long  begin, end;
	int i;

	if ( len < THREAD_NUM )
	{
		begin = ( len>ID ) ? ID : len;
		end = ( len>(ID+1) ) ? ( ID+1 ) : len;
	}
	else
	{
    long  temp=len/THREAD_NUM;
//		begin = ID * len / THREAD_NUM;
	begin = ID *temp;
 //		end = ( ID + 1 ) * len / THREAD_NUM;
	end = ( ID + 1 ) * temp;
	}
	rand_val( ID, ID+1 );

	for ( i = begin; i < end; i++ )
	{
		data[i] = (int)(rand_val(ID,0)*(boundry_b-boundry_a))+boundry_a;
	}

	return NULL;
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
