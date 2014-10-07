u0868472
========

WEIYU LIU

1. Assignment1_SampleSort.cpp:
  This file use mpi_io to read specified data file and then sort the data in it. It does not take any argument. The general method of compiling and executing methods will be good for it as follows.
  #mpic++ Assignment1_SampleSort.cpp -o sampleSort.out
  #mpirun -np 16 sampleSort.out
   
2. GraphColoring.cpp
  This file takes one argument, which is the path of graph file. It will read graph from the specific file and graph it using Jones-Plassmann coloring algorithm based on MPI and then write the result out.
  #mpic++ GraphColoring.cpp -o graphColoring.out
  #mpirun -np 16 graphColoring.out data_file_path
  
3. Maximum Subarray Sum: 
MaximumSubarraySum_divide&conquer.cpp: compute the sum with divide & conquer algorithm.
MaximumSubarraySum_linear.cpp: compute the sum with sequential algorithm.
ParrallelMaximumSubarraySum: best parallel algorithm for the maximal subarray problem. It would produce a random array from -100 to 100. And the length of the array depends on the number of process, which is input when running the MPI script. The reason, why I did not use length of the array as input parameter of the program, is to keep the length of array as n/log(n), with which method, we could reach O(log(n)) complexity. And then it will calculate the maximum subarray sum in parraell algorithm. 
All of these three programs do not take any argument. The specific steps to run it are as follows:
1) build out file with command
$ mpic++ codename -outfile
2) run the out file with command
$ mpirun -#ofProcess outfilename
