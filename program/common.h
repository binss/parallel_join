//20150317  by sheldont
#ifndef __COMMON_H_
#define __COMMON_H_

#define NAMEMAX 256	//文件名最大长度
#include <stdio.h>
#include<omp.h>
#include <string.h>
#include <sys/time.h>


char filenames[][NAMEMAX] = {
								"DataKey0M16M",
							 //	"DataUniform16M","DataUniform32M","DataUniform64M","DataUniform128M","DataUniform256M","DataUniform512M","DataUniform1024M",
							  //  "DataZipfHigh16M","DataZipfHigh32M","DataZipfHigh64M","DataZipfHigh128M","DataZipfHigh256M","DataZipfHigh512M",
								"DataZipfLow16M","DataZipfLow32M","DataZipfLow64M","DataZipfLow128M","DataZipfLow256M","DataZipfLow512M"
					    	};
int data_len[] = {
					1<<24,
					1<<24, 1<<25, 1<<26, 1<<27,1<<28,1<<29,1<<30,
				    1<<24, 1<<25, 1<<26, 1<<27,1<<28,1<<29,
					1<<24, 1<<25, 1<<26, 1<<27,1<<28,1<<29
				};

//读入数据,根据上面文件载入相应的数据
void loaddata(char filenames[][NAMEMAX],int index, int* datalen,unsigned int* key,int *pkeylen);

#endif