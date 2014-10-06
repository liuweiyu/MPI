//
//  main.cpp
//  parallel_best_max_subarray
//
//  Created by hang shao on 9/30/14.
//  Copyright (c) 2014 hang shao. All rights reserved.
//

#include <iostream>
#include <mpi.h>
#include <math.h>
#include <stdlib.h>
int main(int argc,char * argv[])
{
    MPI_Init(&argc, &argv);
    int rank, npes,i;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &npes);
    int length;
    length = exp2(npes);
    int *array = new int[length];
    for ( i =0; i < length; ++i)
    {
        array[i] = rand() % 21 + (-10) ;
        //std::cout<<array[i]<<" ,";
    }
    //std::cout<<std::endl;
    int *prefixSum = new int[length];
    int *subarray = new int[length/npes];
    int *prefix = new int[length];
    if (length == 1 )
        prefixSum[0] = array[0];
    int total_prefix;
    MPI_Scatter (array,length/npes,MPI_INT,subarray,length/npes,MPI_INT,0,MPI_COMM_WORLD);
    prefix[0] = subarray[0];
    for ( i = 0 ; i <(length / npes); i++)
    {
        if (i != 0)
            prefix [i] = prefix[i-1] + subarray[i];
        
    }
    MPI_Scan(&prefix[length/npes-1],&total_prefix,1,MPI_INT,MPI_SUM,MPI_COMM_WORLD );
    for ( i = 0 ; i < length/npes ; i++)
    {
        prefix[i] += total_prefix - prefix[length/npes-1];
    }
    free (prefixSum);
    free (subarray);
    int * min = new int[npes];
    int * cand = new int[length/npes];
    int mini ;
    mini = prefix[0];
    min[0] = 0;
    for (i =0;i<length/npes;i++)
    {
        if (prefix[i] <mini)
        {
            mini = prefix[i];
        }
    }
    MPI_Gather(&mini, 1, MPI_INT, &min[1],1,MPI_INT,0,MPI_COMM_WORLD);
    if (rank == 0)
    {
        for( i = 0;i<npes ;i++)
        {
            if (min[i+1] > min[i])
            {
                min[i+1] = min[i];
            }
            
        }
    }
    int prefix_sum0;
    MPI_Scatter (min,1,MPI_INT,&prefix_sum0,1,MPI_INT,0,MPI_COMM_WORLD);
    //std::cout<<prefix_sum0<< " rank is "<<rank <<std::endl;
    
    mini = prefix_sum0;
    min [0] = mini;
    for(i = 1; i < length/npes; i++)
    {
        if (mini >= prefix[i-1])
        {
            mini = prefix[i-1];
            min[i] = mini;
        }
        
        min[i] = mini;
        cand[i] = prefix[i] - min[i];
        //std::cout<<cand[i]<< " rank is "<<rank <<std::endl;
    }
    free (prefix);
    free(min);
    int max ;
    max = cand[0];
    for(i = 0; i < length/npes; i++)
    {
        if (max <= cand[i])
            max = cand[i];
    }
    free(cand);
    int Max;
    MPI_Scan(&max,&Max,1,MPI_INT,MPI_MAX,MPI_COMM_WORLD );
    if (rank == 1)
    {
        std::cout<<"the maximum subarray sum is : "<<Max<<std::endl;
    }
    MPI_Finalize();
    return 0;
}
