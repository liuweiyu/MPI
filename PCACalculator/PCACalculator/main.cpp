#include <mpi.h>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <stdlib.h>     /* srand, rand */
#include <stdio.h> 
#include <time.h>
#include <string>
#include <vector>
#include <sstream>
using namespace std;

#define MATRIX_FN "E:\\2014 Fall\\Big Data Computer System\\Final Project\\PCACalculator\\PCACalculator\\input\\matrix.txt"
#define COL_NUM 100000
#define ROW_NUM 10
#define ROOT 0

string Int2Str(int i){
	ostringstream s;
	s << i;
	return s.str();
}

int WriteRandomMatrix(int num_rows, int num_cols){
	ofstream outf("E:\\2014 Fall\\Big Data Computer System\\Final Project\\PCACalculator\\PCACalculator\\input\\matrixTxt.txt");
	if(!outf.is_open()){
		cout<<"error in opening file .\\tmp\\matrixTxt.txt";
		return -1;
	}

	srand (time(NULL));
	double* mat = new double[num_rows*num_cols];
	for(int i = 0; i<num_rows*num_cols; i++){
		mat[i] = (double)rand();
		if(i==0 || i%num_cols != 0){
			outf<<mat[i]<<"\t";
		}else{
			outf<<endl<<mat[i]<<"\t";
		}
	}

	FILE* fp = fopen(MATRIX_FN, "rb");
	fp = fopen(MATRIX_FN, "wb");
//	fprintf(fp, "%d %d\n", num_rows, num_cols);
	fwrite (mat, sizeof(double), num_rows*num_cols, fp);
	fclose(fp);

	return 0;
}

/*
@parameters: s: string to be splitted
			 token: separator
@funtion: split s by token into a vector<string> type variable
@return: vector with string elements
*/
vector<string> StringSplit(string& s, const char* token){
	char *cstr, *p, *next_p = NULL;
    vector<string> res;
    cstr = new char[s.size()+1];
    strcpy_s(cstr, s.size()+1, s.c_str());
    p = strtok_s(cstr, token, &next_p);
    while(p!=NULL){		
        res.push_back(p);
        p = strtok_s(NULL, token, &next_p);
    }
	delete[] cstr;
	cstr=NULL;	
	p=NULL;
	next_p = NULL;

    return res;
}

int ReadMatrixPartition(int rank, int nproc){
	ifstream inf(MATRIX_FN);
	if(!inf.is_open()){
		cout<<"error in opening file "<<MATRIX_FN<<endl;
		return -1;
	}else{
		string line = "";
		int count = 0;
		while(getline(inf, line)){
			if(count<rank){
				count++;
				continue;
			}else{
				vector<string> elements = StringSplit(line, "\t");

			}

			count++;
		}
	}
}

int main(int argc, char* argv[]){
//	WriteRandomMatrix(COL_NUM, ROW_NUM);

	clock_t start;
	start = clock();

	int rank, nproc;
	MPI_Init(&argc, & argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &nproc);

	//read partial matrix in
	MPI_File fh;
	MPI_Status status;
	MPI_File_open(MPI_COMM_WORLD, MATRIX_FN, MPI_MODE_RDONLY, MPI_INFO_NULL, &fh);

	MPI_Offset filesize = 0;	
	MPI_File_get_size(fh, &filesize);	

	filesize /= sizeof(double);
	cout<<Int2Str(rank)<<", fileSize is "<<filesize<<endl;
	int avgVectorNum = COL_NUM/nproc;//num of elements to be sorted in this process	
	int tmpVectorNum = avgVectorNum;
	if(rank == nproc-1){
		tmpVectorNum = COL_NUM - (nproc-1)*avgVectorNum;
	}
	cout<<Int2Str(rank)<<", vector number is "<<tmpVectorNum<<endl;

	double* subarray = (double*)malloc((tmpVectorNum*ROW_NUM)*sizeof(double));
	MPI_File_seek(fh, rank*(avgVectorNum*ROW_NUM)*sizeof(double), MPI_SEEK_SET);
	MPI_File_read(fh, subarray, tmpVectorNum*ROW_NUM, MPI_DOUBLE, &status);
	MPI_File_close(&fh);

	double* localL = (double*)malloc(ROW_NUM*sizeof(double));
	for(int i = 0; i<ROW_NUM; i++){
		localL[i] = 0;
	}
	double* localG = (double*)malloc(ROW_NUM*ROW_NUM*sizeof(double));
	for(int i = 0; i<ROW_NUM*ROW_NUM; i++){
		localG[i] = 0;
	}

	//calculate local L
	for(int i = 0; i < ROW_NUM; i++){
		for(int j = 0; j < tmpVectorNum; j++){
			localL[i] += subarray[i+ROW_NUM*j];
		}
	}

	//calculate local G
	for(int i = 0; i<ROW_NUM*ROW_NUM; i++){
		int tmpRowNum = i/ROW_NUM;
		int tmpColNum = i%ROW_NUM;

		for(int j = 0; j<ROW_NUM; j++){
			for(int k = 0; k<tmpVectorNum; k++){
				localG[i] += subarray[tmpRowNum+ROW_NUM*k]*subarray[tmpColNum+ROW_NUM*k];
			}
		}
	}

	free(subarray);

	/*ofstream outf2;
	string tmpFileName = "E:\\2014 Fall\\Big Data Computer System\\Final Project\\PCACalculator\\PCACalculator\\tmp\\localL_" + Int2Str(rank) + ".txt";
	outf2.open(tmpFileName);
	for(int i = 0; i<ROW_NUM; i++){
		outf2<<localL[i]<<endl;
	}
	outf2.clear();
	outf2.close();

	ofstream outf3;
	string tmpFileName2 = "E:\\2014 Fall\\Big Data Computer System\\Final Project\\PCACalculator\\PCACalculator\\tmp\\localG_" + Int2Str(rank) + ".txt";
	outf3.open(tmpFileName2);
	for(int i = 0; i<ROW_NUM*ROW_NUM; i++){
		if(i==0 || i%ROW_NUM!=0){
			outf3<<localG[i]<<"\t";
		}else{
			outf3<<endl<<localG[i]<<"\t";
		}
	}
	outf3.clear();
	outf3.close();*/

	//Gather all local L
	double* allLocalL = NULL;
	if(rank == ROOT){
		allLocalL = (double*)malloc(nproc*ROW_NUM*sizeof(double));	
	}
	MPI_Gather(localL, ROW_NUM, MPI_DOUBLE, allLocalL, ROW_NUM, MPI_DOUBLE, ROOT, MPI_COMM_WORLD);
	cout<<rank<<": after gather L"<<endl;
	free(localL);

	//Gather all local G
	double* allLocalG = NULL;
	if(rank == ROOT){
		allLocalG = (double*)malloc(nproc*ROW_NUM*ROW_NUM*sizeof(double));
	}
	MPI_Gather(localG, ROW_NUM*ROW_NUM, MPI_DOUBLE, allLocalG, ROW_NUM*ROW_NUM, MPI_DOUBLE, ROOT, MPI_COMM_WORLD);
	cout<<rank<<": after gather G"<<endl;
	free(localG);

	if(rank == ROOT){
		//calculate global L in ROOT node
		double* globalL = new double[ROW_NUM];
		if(rank == ROOT){		
			for(int i = 0; i<ROW_NUM; i++){
				globalL[i] = 0;
				for(int j = 0; j<nproc; j++){
					globalL[i] += allLocalL[ROW_NUM*j + i];
				}
			}		
		}	

		/*ofstream outf;
		string tmpFileName3 = "E:\\2014 Fall\\Big Data Computer System\\Final Project\\PCACalculator\\PCACalculator\\tmp\\globalL_" + Int2Str(rank) + ".txt";
		outf.open(tmpFileName3);
		for(int i = 0; i<ROW_NUM; i++){
			outf<<globalL[i]<<endl;
		}
		outf.clear();
		outf.close();	*/	

		//calculate global G
		double* globalG = new double[ROW_NUM*ROW_NUM];
		if(rank == ROOT){
			for(int i = 0; i < ROW_NUM*ROW_NUM; i++){
				globalG[i] = 0;
				for(int j = 0; j<nproc; j++){
					globalG[i] += allLocalG[j*ROW_NUM*ROW_NUM + i]; 
				}
			}		
		}

		/*ofstream outf4;
		string tmpFileName4 = "E:\\2014 Fall\\Big Data Computer System\\Final Project\\PCACalculator\\PCACalculator\\tmp\\globalG_" + Int2Str(rank) + ".txt";
		outf4.open(tmpFileName4);
		for(int i = 0; i<ROW_NUM*ROW_NUM; i++){
			if(i==0 || i%ROW_NUM!=0){
				outf4<<globalG[i]<<"\t";
			}else{
				outf4<<endl<<globalG[i]<<"\t";
			}
		}
		outf4.clear();
		outf4.close();*/
	
		free(allLocalL);
		free(allLocalG);

		double** Rou = (double**)malloc(ROW_NUM*sizeof(double*));
		for(int i = 0; i<ROW_NUM; i++){
			Rou[i] = (double*)malloc(ROW_NUM*sizeof(double));
			for(int j = 0; j<ROW_NUM; j++){
				double denominator = sqrt(COL_NUM*globalG[i*ROW_NUM+i]-globalL[i]*globalL[i])*sqrt(COL_NUM*globalG[j*ROW_NUM+j]-globalL[j]*globalL[j]);
				Rou[i][j] = (COL_NUM * globalG[i*ROW_NUM+j]-globalL[i]*globalL[j])/denominator;
			}
		}

		ofstream outf5;
		string tmpFileName5 = "E:\\2014 Fall\\Big Data Computer System\\Final Project\\PCACalculator\\PCACalculator\\tmp\\rou_" + Int2Str(rank) + ".txt";
		outf5.open(tmpFileName5);
		for(int i = 0; i<ROW_NUM; i++){
			for(int j = 0; j<ROW_NUM; j++){
				outf5<<Rou[i][j]<<"\t";
			}
			outf5<<endl;
		}
		outf5.clear();
		outf5.close();

		free(globalL);
		free(globalG);
	}	
	
	double time = (double)(clock() - start)/CLOCKS_PER_SEC;
	cout<<rank<<" takes time: "<<time<<endl;
	MPI_Finalize();

	return 0;
}