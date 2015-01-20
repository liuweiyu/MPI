u0868472
========

WEIYU LIU

1. Assignment1_SampleSort.cpp:
  This file use mpi_io to read specified data file and then sort the data in it. It does not take any argument. The general method of compiling and executing methods will be good for it as follows.

  #mpic++ Assignment1_SampleSort.cpp -o sampleSort.out
  #mpirun -np 16 sampleSort.out

  This code was finished with my course partner, Hang Shao.
   
2. GraphColoring.cpp
  This file takes one argument, which is the path of graph file. It will read graph from the specific file and graph it using Jones-Plassmann coloring algorithm based on MPI and then write the result out.
  #mpic++ GraphColoring.cpp -o graphColoring.out
  #mpirun -np 16 graphColoring.out data_file_path
