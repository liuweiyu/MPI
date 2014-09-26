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
#define INPUT_FILE "$SCRATCH/data/sort_data.dat"
#define INPUT_FILE_DEBUG "$SCRATCH/data/sort_data_debug.dat"
#define INPUT_FILE_SECOND "$SCRATCH/data/sort_alt_data.dat"

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

static void root(int totalSize, int npes){    
    //gather samples from slave process
    const int sampleNum = npes-2;
    int* sendArray = new int[sampleNum];
    int *rbuf = (int*)malloc((npes)*sampleNum*sizeof(int));
    
    MPI_Gather (sendArray, sampleNum, MPI_INT, rbuf, sampleNum, MPI_INT, ROOT, MPI_COMM_WORLD);
    cout<<"root get samples"<<endl;
	int* samples = (int*)malloc((npes-1)*sampleNum*sizeof(int));
	for(int i = 0; i<npes-1; i++){
		samples[i] = rbuf[i+sampleNum];
	}
	free(rbuf);
	//for(int p = 0; p<sampleNum*(npes-1); p++){		
	//	cout<<"root "<<Int2Str(samples[p])<<"\t";
	//}
	//cout<<endl;
    //sort samples
//    Sort(rbuf, (npes-1)*sampleNum);
    qsort(samples, (npes-1)*sampleNum, sizeof(int), compare);
    //pick splitters
    int* pickedSplitters = (int*)malloc(sampleNum*sizeof(int));
    PickSplitters(samples, pickedSplitters, (npes-1)*sampleNum, sampleNum);
    free(samples);

    //broadcast splitters
	cout<<"root get splitters"<<endl;
	for(int i = 0; i<sampleNum; i++){
		cout<<"root "<<Int2Str(pickedSplitters[i])<<"\t";
	}
	cout<<endl;
	
    MPI_Bcast(pickedSplitters, sampleNum, MPI_INT, ROOT, MPI_COMM_WORLD);

	cout<<"root broadcasted"<<endl;
    free(pickedSplitters);
	int * scount = (int *) malloc (npes* sizeof(int));
    int * rcount = (int *) malloc (npes* sizeof(int));
	for(int j = 0; j<npes; j++){
		scount[j]=0;
		rcount[j]=0;
	}
	MPI_Alltoall(scount, 1, MPI_INT, rcount, 1, MPI_INT, MPI_COMM_WORLD);
	cout<<"root all to all"<<endl;

	int * sdispls = (int *) malloc (npes* sizeof(int));
    int * rdispls = (int *) malloc (npes* sizeof(int));
	int * subarray = (int *) malloc (npes* sizeof(int)); 
	for(int l = 0; l<npes; l++){
		subarray[l]=0;
		sdispls[l]=0;
		rdispls[l]=0;
	}
	
	for (int i =1; i < npes-1;++i){
		sdispls[i+1] = scount[i] + sdispls[i];
    }
	int subSize = totalSize/(npes-1);
    sdispls[npes-1] = std::min(sdispls[npes-2]+scount[npes-2],subSize-1);         
   
    int length = 0;
    for (int i =1; i < npes-1; ++i){
        rdispls[i+1] = rcount[i]+rdispls[i];		
		length += rcount[i];
    }
    rdispls[npes-1] = std::min(rdispls[npes-2]+rcount[npes-2],subSize-1);
    length += rcount[npes-1];

	int * subarray_sorted = (int *) malloc (length * sizeof(int));
	for(int i = 0; i<length; i++){
		subarray_sorted[i]=0;
	}	
	
    MPI_Alltoallv(subarray, scount, sdispls, MPI_INT, subarray_sorted, rcount, rdispls, MPI_INT, MPI_COMM_WORLD);

	free(subarray);        
    free(sdispls);
    free(rdispls);
    free(scount);
    free(rcount);
	free(subarray_sorted);
}

static void slave(int totalSize, int rank, int npes){

    int i,j;           
    int slaveNum = npes-1;
    int subSize = totalSize/slaveNum;
	
    //build array with random integers
    int * subarray = (int *) malloc (subSize * sizeof(int) ); 
    int * spliters = (int *) malloc ((npes-2)* sizeof(int));
    int * sample = (int *) malloc((npes-2)*sizeof(int));    
	
    srand(time(NULL));
	ofstream outf;
	string outFN = "E:\\2014 Fall\\Big Data Computer System\\Assignment1\\MPITest\\MPITest\\input\\num_" + Int2Str(rank) + ".txt"; 
	outf.open(outFN);
	if(!outf.is_open()){
//		cout<<"error in opening file " << outFN<<endl;
		return;
	}
    outf<<"current rank is "<<Int2Str(rank)<<endl;
	outf<<"subSize is "<<Int2Str(subSize)<<endl;
    for (i=0; i< subSize; ++i){
        subarray[i] = rand();
	    outf<<subarray[i]<<endl;
    }
	outf.clear();
	outf.close();
 //   cout<<endl;
	
    // sort the local array
    qsort(subarray, subSize, sizeof(int), compare);
    cout<<"sorted subarray"<<endl;
	
    //pick samples and send them to root
    int sampleNum = npes-2;	
    PickSplitters(subarray, sample, subSize, sampleNum);	
	//for(int p = 0; p<sampleNum; p++){
	//	cout<<sample[p]<<"\t";
	//}
	//cout<<endl;

    int* rbuf;
	rbuf = new int[(npes-1)*sampleNum];
    MPI_Gather(sample, sampleNum, MPI_INT, rbuf, (npes-1)*sampleNum, MPI_INT, ROOT, MPI_COMM_WORLD);
    free(sample);
	cout<<"sent samples to root"<<endl;
    
    //Broadcast spliters into each process
    MPI_Bcast(spliters, sampleNum, MPI_INT, ROOT, MPI_COMM_WORLD);
	cout<<Int2Str(rank)<<": broadcasted"<<endl;

	//for(int n = 0; n<sampleNum; n++){
	//	cout<<Int2Str(rank)<<": "<<Int2Str(spliters[n])<<"\t";
	//}
	//cout<<endl;

	int * sdispls = (int *) malloc (npes* sizeof(int));
    int * rdispls = (int *) malloc (npes* sizeof(int));
    int * scount = (int *) malloc (npes* sizeof(int));
    int * rcount = (int *) malloc (npes* sizeof(int));   
 
	for(int l = 0; l<npes; l++){
		sdispls[l]=0;
		rdispls[l]=0;
		scount[l]=0;
		rcount[l]=0;
	}

    for (i = 0, j = 0;i < subSize & j < npes-2;){
	if (subarray[i] <= spliters[j]){
		i++;
		scount[j+1]++;
	}else
		j++;
    }	
    free(spliters);
	
    scount[npes-1] = subSize - i;

	//for(int q = 0; q<npes; q++){
	//	cout<<Int2Str(rank)<<": "<<Int2Str(scount[q])<<"\t";
	//}
	//cout<<endl;

    for (i =1; i < npes-1;++i){
		sdispls[i+1] = scount[i] + sdispls[i];
    }
    sdispls[npes-1] = std::min(sdispls[npes-2]+scount[npes-2],subSize-1);         
	
    MPI_Alltoall(scount, 1, MPI_INT, rcount, 1, MPI_INT, MPI_COMM_WORLD);//exchage between sub-processes
    int length = 0;
    for (i =1; i < npes-1;++i){
        rdispls[i+1] = rcount[i]+rdispls[i];		
		length += rcount[i];
    }
    rdispls[npes-1] = std::min(rdispls[npes-2]+rcount[npes-2],subSize-1);
    length += rcount[npes-1];

	/*for(int q = 0; q<npes; q++){
		cout<<Int2Str(rank)<<": rdispls "<<Int2Str(rdispls[q])<<"\t";
	}
	cout<<endl;

	for(int q = 0; q<npes; q++){
		cout<<Int2Str(rank)<<": rcount "<<Int2Str(rcount[q])<<"\t";
	}
	cout<<endl;
	for(int q = 0; q<npes; q++){
		cout<<Int2Str(rank)<<": scount "<<Int2Str(scount[q])<<"\t";
	}
	cout<<endl;

	cout<<Int2Str(rank)<<": length "<<length<<endl;*/

    int * subarray_sorted = (int *) malloc (length * sizeof(int));
    MPI_Alltoallv(subarray, scount, sdispls, MPI_INT, subarray_sorted, rcount, rdispls, MPI_INT, MPI_COMM_WORLD);
   
    //local sort in each process
    qsort(subarray_sorted, length, sizeof(int), compare);
	
    free(subarray);        
    free(sdispls);
    free(rdispls);
    free(scount);
    free(rcount);

	string resultFN = "E:\\2014 Fall\\Big Data Computer System\\Assignment1\\MPITest\\MPITest\\output\\result_"+Int2Str(rank)+".txt";
	outf.open(resultFN);
	if(!outf.is_open()){
		cout<<"error in opening file "<<endl;
		return;
	}

	for(int m = 0; m<length; m++){
		outf<<subarray_sorted[m]<<endl;
	}
	outf.clear();
	outf.close();

	free(subarray_sorted);
}

static void slaveWithMPIIO(int rank, int npes){
    int i,j;
    int slaveNum = npes-1;
    
    MPI_File fh;
    MPI_Status status;
    MPI_File_open(MPI_COMM_WORLD, INPUT_FILE_DEBUG, MPI_MODE_RDONLY, MPI_INFO_NULL, &fh);
		
    MPI_Offset filesize;
    MPI_File_get_size(fh, &filesize);
    filesize /= sizeof(int);
//    MPI_Offset offset = filesize/slaveNum;
    int size_array = filesize/slaveNum;//num of elements to be sorted in this process
    if(rank==slaveNum){
//	offset = filesize - (slaveNum-1)*size_array;
       	size_array = filesize - (slaveNum-1)*size_array;
    }

    int* subarray = (int*)malloc((size_array)*sizeof(int));

    MPI_File_seek(fh, rank*size_array*sizeof(int), MPI_SEEK_SET);
    MPI_File_read(fh, subarray, size_array, MPI_INT, &status);
    MPI_File_close(&fh);
    
    //main body for sorting	
    int * spliters = (int *) malloc ((npes-2)* sizeof(int));
    int * sample = (int *) malloc((npes-2)*sizeof(int));
    int * sdispls = (int *) malloc (npes* sizeof(int));
    int * rdispls = (int *) malloc (npes* sizeof(int));
    int * scount = (int *) malloc (npes* sizeof(int));
    int * rcount = (int *) malloc (npes* sizeof(int));
	
    // sort the local array
    qsort(subarray, size_array, sizeof(int), compare);
    
    //pick samples and send them to root
    int sampleNum = npes-2;	
    PickSplitters(subarray, sample, size_array, sampleNum);	
    int* rbuf;
    MPI_Gather (sample, sampleNum, MPI_INT, rbuf, (npes-1)*sampleNum, MPI_INT, ROOT, MPI_COMM_WORLD);
    free(sample);
	
    //get spliters from root
    MPI_Bcast(spliters, npes-2, MPI_INT, ROOT, MPI_COMM_WORLD);	
    
 /*   memset(sdispls, 0, npes);
    memset(scount, 0, npes);
    memset(rdispls, 0, npes);*/
	for(int l = 0; l<npes; l++){
		sdispls[l]=0;
		rdispls[l]=0;
		scount[l]=0;
		rcount[l]=0;
	}
    for (i = 0, j = 0; i < size_array & j < npes-2;){
	if (subarray[i] <= spliters[j]){ 
	    i++;
	    scount[j+1]++;			
	}else{	
	    j++;
	}
    }

    scount[slaveNum] = size_array - i;

    for (i =1; i < slaveNum;++i){
	sdispls[i+1] = scount[i] + sdispls[i];
    }
    sdispls[slaveNum] = std::min(sdispls[npes-2]+scount[npes-2],size_array-1);        
    
    MPI_Alltoall(scount, 1, MPI_INT, rcount, 1, MPI_INT, MPI_COMM_WORLD);//exchage between sub-processes
	
    //get length of elements to be received
    int length = 0;
    for (i =1; i < npes-1;++i){
        rdispls[i+1] = rcount[i]+rdispls[i];
        length += rcount[i];
    }
    rdispls[npes-1] = std::min(rdispls[npes-2]+rcount[npes-2],size_array-1);
    length += rcount[npes-1];

    int * subarray_sorted = (int *) malloc (length * sizeof(int) );
    MPI_Alltoallv(subarray, scount, sdispls, MPI_INT, subarray_sorted, rcount, rdispls, MPI_INT, MPI_COMM_WORLD);
    
    //local sort in each process
    qsort(subarray_sorted, length, sizeof(int), compare);   
    
    //gather number counts from all other processes
    int* rbuf2 = new int[npes];
    MPI_Allgather(&length, 1, MPI_INT, rbuf2, 1, MPI_INT, MPI_COMM_WORLD);
    //write result out
    MPI_File_open(MPI_COMM_WORLD, OUTPUT_FILE, MPI_MODE_CREATE|MPI_MODE_WRONLY, MPI_INFO_NULL, &fh);
    
    int offset = 0;
    for(int i = 1; i<=rank-1; i++){
        offset += rbuf2[i];
    }
    delete[] rbuf2;
    rbuf2 = NULL;
    
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
}

int main(int argc, char** argv){
    //Initialize
    MPI_Init(&argc, &argv);
    
    int rank, npes;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &npes);
    
	int size = atoi(argv[1]);
    if(rank==ROOT){
        root(size, npes);
    }else{	
		slave(size, rank, npes);
		//slaveWithMPIIO(rank, npes);
    }
    
    MPI_Finalize();
    return 0;
}
