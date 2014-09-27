#include <stdio.h>
#include <stdlib.h>
#include <algorithm>
#include <mpi.h>
#include <time.h>
#include <string.h>
#include <iostream>
#include <fstream>
#include <sstream>
using namespace std;

#define ROOT 0
#define INPUT_FILE "/uufs/chpc.utah.edu/common/home/ci-water4-0/CS6965/data/sort_data.dat"
#define INPUT_FILE_DEBUG "/uufs/chpc.utah.edu/common/home/ci-water4-0/CS6965/data/sort_data_debug.dat"
#define INPUT_FILE_DEBUG_WIN "E:\\2014 Fall\\Big Data Computer System\\Assignment1\\MPITest\\MPITest\\input\\sort_data_debug.dat"
#define INPUT_FILE_SECOND "/uufs/chpc.utah.edu/common/home/ci-water4-0/CS6965/data/sort_alt_data.dat"

#define OUTPUT_FILE "/uufs/chpc.utah.edu/common/home/u0868472/assignment1/result.dat"

string Int2Str(int i){
	ostringstream s;
	s << i;
	return s.str();
}

int compare (const void * a, const void * b) {
    return ( *(int*)a - *(int*)b );
}

void PickSplitters(int* rbuf, int* pickedSplitters, int totalNum, int splitterNum){
    int interval = totalNum/(splitterNum+1);
    for(int i = 0; i< splitterNum; i++){
        int tmpSplitterIndex = (i+1)*interval;
        int tmpSplitter = rbuf[tmpSplitterIndex];
        pickedSplitters[i] = tmpSplitter;
    }
}

int main(int argc, char** argv){
	//initialize mpi
	MPI_Init(&argc, &argv);  
    int rank, npes;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &npes);

	//initialize variables
    int i,j;
    int slaveNum = npes-1;
	int sampleNum = npes-2;	
	int * spliters = (int *) malloc (sampleNum* sizeof(int));
    int * sample = (int *) malloc(sampleNum*sizeof(int));
    int * sdispls = (int *) malloc (npes* sizeof(int));
    int * rdispls = (int *) malloc (npes* sizeof(int));
    int * scount = (int *) malloc (npes* sizeof(int));
    int * rcount = (int *) malloc (npes* sizeof(int));
	for(int k=0; k<npes; k++){
		sdispls[k]=0;
		rdispls[k]=0;
		scount[k]=0;
		rcount[k]=0;
	}
    
	//slave processes read data file, local sort and pick samples
	int* subarray;
	if(rank==ROOT){
		subarray = (int *) malloc (npes* sizeof(int)); 
	}	
	
	MPI_File fh;
	MPI_Status status;
    MPI_File_open(MPI_COMM_WORLD, INPUT_FILE_DEBUG, MPI_MODE_RDONLY, MPI_INFO_NULL, &fh);
//	MPI_File_open(MPI_COMM_WORLD, INPUT_FILE_DEBUG_WIN, MPI_MODE_RDONLY, MPI_INFO_NULL, &fh);
	
	MPI_Offset filesize = 0;	
	MPI_File_get_size(fh, &filesize);
	cout<<Int2Str(rank)<<", fileSize is "<<filesize<<endl;	

	filesize /= sizeof(int);
	int size_array = filesize/slaveNum;//num of elements to be sorted in this process	
	if(rank==ROOT){
		size_array=0;
	}else if(rank==slaveNum){
       	size_array = filesize - (slaveNum-1)*size_array;		
	}

	subarray = (int*)malloc((size_array)*sizeof(int));
	MPI_File_seek(fh, (rank-1)*size_array*sizeof(int), MPI_SEEK_SET);
	MPI_File_read(fh, subarray, size_array, MPI_INT, &status);
	MPI_File_close(&fh);	

    if(rank!=ROOT){
		//first local sort
		qsort(subarray, size_array, sizeof(int), compare);

		//pick samples and send them to root
		PickSplitters(subarray, sample, size_array, sampleNum);			
	}
   
    int* rbuf = (int*)malloc((npes)*sampleNum*sizeof(int));
    MPI_Gather(sample, sampleNum, MPI_INT, rbuf, sampleNum, MPI_INT, ROOT, MPI_COMM_WORLD);
	free(sample);

	//root sorts samples and picks splitters
	if(rank == ROOT){
		int* allSamples = (int*)malloc((npes-1)*sampleNum*sizeof(int));
		for(int i = 0; i<npes-1; i++){
			allSamples[i] = rbuf[i+sampleNum];
		}
		free(rbuf);

		//sort splitters
		qsort(allSamples, (npes-1)*sampleNum, sizeof(int), compare);

		//pick splitters
		int* pickedSplitters = (int*)malloc(sampleNum*sizeof(int));
		PickSplitters(allSamples, pickedSplitters, (npes-1)*sampleNum, sampleNum);
		free(allSamples);

		MPI_Bcast(pickedSplitters, sampleNum, MPI_INT, ROOT, MPI_COMM_WORLD);	
		free(pickedSplitters);
	}    
	
    //get spliters from root
    if(rank!=ROOT){
		MPI_Bcast(spliters, sampleNum, MPI_INT, ROOT, MPI_COMM_WORLD);	
	
		//calculate scount, rcount and exchange it between processes
		for (i = 0, j = 0; i < size_array & j < sampleNum;){
			if (subarray[i] <= spliters[j]){
				i++;
				scount[j+1]++;			
			}else{	
				j++;
			}
		}

		scount[slaveNum] = size_array - i;
	}	 
    
    MPI_Alltoall(scount, 1, MPI_INT, rcount, 1, MPI_INT, MPI_COMM_WORLD);//exchage between sub-processes	
	
    //get length of elements to be received
	for (i =1; i < slaveNum;++i){
		sdispls[i+1] = scount[i] + sdispls[i];
	}
	sdispls[slaveNum] = std::min(sdispls[sampleNum]+scount[sampleNum],size_array-1);

    int length = 0;
    for (i =1; i < npes-1;++i){
        rdispls[i+1] = rcount[i]+rdispls[i];
        length += rcount[i];
    }
    rdispls[npes-1] = std::min(rdispls[npes-2]+rcount[npes-2],size_array-1);
    length += rcount[npes-1];

    int * subarray_sorted = (int *) malloc (length * sizeof(int) );
	for(i=0; i<length;i++){
		subarray_sorted[i]=0;
	}
	cout<<"rank "<<Int2Str(rank)<<", length "<<length<<endl;
	MPI_Alltoallv(subarray, scount, sdispls, MPI_INT, subarray_sorted, rcount, rdispls, MPI_INT, MPI_COMM_WORLD);
		
	//local sort in each process
	qsort(subarray_sorted, length, sizeof(int), compare);   
    
	//gather number counts from all other processes
	int* rbuf2 = new int[npes];
	MPI_Allgather(&length, 1, MPI_INT, rbuf2, 1, MPI_INT, MPI_COMM_WORLD);
	//write result out
	MPI_File_open(MPI_COMM_WORLD, OUTPUT_FILE, MPI_MODE_CREATE|MPI_MODE_WRONLY, MPI_INFO_NULL, &fh);
//	MPI_File_open(MPI_COMM_WORLD, "..\\MPITest\\output\\result.txt", MPI_MODE_CREATE|MPI_MODE_WRONLY, MPI_INFO_NULL, &fh);
		
	int offset = 0;
	for(int i = 0; i<rank; i++){
		offset += rbuf2[i];
	}
	delete[] rbuf2;
	rbuf2 = NULL;
	cout<<rank<<"offset: "<<offset<<endl;
    
	offset *= sizeof(int);
	MPI_File_write_at(fh, offset, subarray_sorted, length, MPI_INT, &status);    
	MPI_File_close(&fh);

    free(subarray);
    free(subarray_sorted);
    free(spliters);
    free(sdispls);
    free(rdispls);
    free(scount);
    free(rcount);

	MPI_Finalize();
	return 0;
}
