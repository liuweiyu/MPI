#include <map>
#include <set>
#include <vector>
#include <mpi.h>
#include <fstream>
#include <iostream>
#include <string>
using namespace std;

#define MASTER 0 

map<int, set<int>> nodeNeighborsMap;
int beginNode;
int endNode;
int* colorQueue;
set<int> globalVertices;
set<int> localVertices;
set<int> coloredNodes;

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

void InsertIntoPartitionedNodeNeighborMap(int node1, int node2){
	if(nodeNeighborsMap.find(node1)==nodeNeighborsMap.end()){
		set<int> tmp;
		tmp.insert(tmp.end(), node2);
		nodeNeighborsMap.insert(pair<int, set<int>>(node1, tmp));
	}else{
		(nodeNeighborsMap.at(node1)).insert((nodeNeighborsMap.at(node1)).end(), node2);
	}
}

void GetPartitionedNodeNeighborInfo(string fileName){
	ifstream file;
	file.open(fileName);
	if(!file.is_open()){
		cout<<"error in opening file "<<fileName<<endl;
		return;
	}else{
		string line = "";
		while(getline(file, line)){
			vector<string> elements = StringSplit(line, " ");
			if(elements.at(0) == "e"){
				int node1 = atoi((elements.at(1)).c_str());
				int node2 = atoi((elements.at(2)).c_str());
				if(node1>=beginNode && node1 <=endNode){
					InsertIntoPartitionedNodeNeighborMap(node1, node2);
				}
				if(node2>=beginNode && node2 <=endNode){
					InsertIntoPartitionedNodeNeighborMap(node2, node1);
				}
			}
		}
		file.clear();
		file.close();
	}
}

void InsertNeighbors(set<int>* tmpNeighbors, map<int, int>* nodesCountMap){
	set<int>::iterator it = tmpNeighbors->begin();
	while(it!=tmpNeighbors->end()){
		if(coloredNodes.find(*it) != coloredNodes.end()){
			continue;
		}

		if(find(nodesCountMap->begin(), nodesCountMap->end(), *it) == nodesCountMap->end()){			
			nodesCountMap->insert(pair<int,int>(*it, 1));
		}else{
			(nodesCountMap->at(*it))++;
		}
		it++;
	}
}

int FindMostDegreeVertice(map<int, int>* nodesCountMap){
	int mostDegreeVertice = 0;
	int mostDegree = -1;

	map<int,int>::iterator it = nodesCountMap->begin();
	while(it!=nodesCountMap->end()){
		if(mostDegree = -1 || it->second>mostDegree){
			mostDegreeVertice = it->first;
			mostDegree = it->second;
		}
		it++;
	}

	return mostDegreeVertice;
}

int GetColoredNodeNum(set<int>* nodes){
	int num = 0;
	set<int>::iterator it = nodes->begin();
	while(it!=nodes->end()){
		if(coloredNodes.find(*it)!=coloredNodes.end()){
			num++;
		}
		it++;
	}
	return num;
}

int FindNexVertice(set<int>* nodes){
	int nextVertice = -1;
	int maxDegree = -1;
	set<int>::iterator it = nodes->begin();
	while(it != nodes->end()){
		set<int> tmpNeighbors = nodeNeighborsMap.at(*it);
		int tmpColoredNodeNum = GetColoredNodeNum(&tmpNeighbors);
		if(maxDegree == -1 || maxDegree<tmpColoredNodeNum){
			maxDegree = tmpColoredNodeNum;
			nextVertice = *it;
		}
		it++;
	}

	return nextVertice;
}

int GetColor(int vertice){
	int color = 0;
	set<int> neighbors = nodeNeighborsMap.at(vertice);
	set<int>::iterator it = neighbors.begin();
	while(it!=neighbors.end()){
		int tmpIndex = *it-beginNode;
		int tmpColor = colorQueue[tmpIndex];
		if(tmpColor == color){
			color++;
		}
		it++;
	}

	return color;
}

void ColorVerticesSequentially(set<int>* nodes){	
	while(nodes->size() > 0){
		int nextVertice = FindNexVertice(nodes);
		int color = GetColor(nextVertice);
		colorQueue[nextVertice-beginNode] = color;
		coloredNodes.insert(coloredNodes.end(), nextVertice);
		nodes->erase(nextVertice);
	}
}

void SetNodeIndexMap(int* nodeIndexMap, int* partitionedNodes, int partitionedNodeNum){
	for(int i = 0; i< partitionedNodeNum; i++){
		int tmpNode = partitionedNodes[i];
		nodeIndexMap[tmpNode] = i;
	}
}

int GetNodeNum(char* fileName){
	int nodeNum = 0;
	ifstream file;		
	file.open(fileName);
	if(!file.is_open()){
		cout<<"error in opening file " << fileName << endl;
	}else{
		string line = "";
		while(getline(file, line)){
			vector<string> elements = StringSplit(line, " ");
			if(elements.at(0) == "p"){
				nodeNum = atoi((elements.at(2)).c_str());
				break;
			}
		}

		file.clear();
		file.close();
	}
}

void GetGlobalAndLocalVertices(){
	for(int i = beginNode; i<=endNode; i++){
		set<int> tmpNeighbors = nodeNeighborsMap.at(i);
		set<int>::iterator it = tmpNeighbors.begin();
		bool isGlobal = false;
		while(it!=tmpNeighbors.end()){
			if(*it>endNode || *it<beginNode){
				globalVertices.insert(globalVertices.end(), i);
				isGlobal = true;
				break;
			}
			it++;
		}
		if(!isGlobal){
			localVertices.insert(localVertices.end(), i);
		}
	}
}

int main(int argc, char** argv){	
	int totalNodeNum = 0;	
	int coloredNodeNum = 0;
	
	//initialize mpi
	MPI_Init(&argc, &argv); 
    int rank, npes;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &npes);

	char* fileName;
	if(rank == MASTER){
		fileName = "E:\\2014 Fall\\Big Data Computer System\\Assignment1\\GraphColoring\\GraphColoring\\input\\le450_5a.col";
		totalNodeNum = GetNodeNum(fileName);
	}

	int* sendbuf = (int*)malloc(npes*2*sizeof(int));
	if(rank == MASTER){
		int nodeNumMean = totalNodeNum/npes;
		for(int i = 0; i<npes; i++){
			int tmpStart = i*nodeNumMean + 1;
			int tmpEnd;
			if(i == npes-1){
				tmpEnd = totalNodeNum;
			}else{
				tmpEnd = tmpStart + nodeNumMean - 1;
			}

			sendbuf[2*i] = tmpStart;
			sendbuf[2*i+1] = tmpEnd;
		}
	}else{
		for(int i = 0; i<npes*2; i++){
			sendbuf[i]=0;
		}
	}
	int* rbuf = (int*)malloc(2*sizeof(int));
	for(int i = 0; i<2; i++){
		rbuf[i]=0;
	}
	MPI_Scatter(sendbuf, 2, MPI_INT, rbuf, 2, MPI_INT, MASTER, MPI_COMM_WORLD);

	beginNode = rbuf[0];
	endNode = rbuf[1];
	GetPartitionedNodeNeighborInfo(fileName);

	GetGlobalAndLocalVertices();

	colorQueue = new int[endNode-beginNode+1];
	for(int i = 0; i<endNode-beginNode+1; i++){
		colorQueue[i] = -1;
	}
	
	//color global vertices	
	int* n_wait = (int*)malloc((globalVertices.size())*sizeof(int));
	set<int>* send_queue = new set<int>[globalVertices.size()];
	set<int> color_queue;

	set<int>::iterator git = globalVertices.begin();		
	while(git!=globalVertices.end()){
		int index = *git-beginNode;
		n_wait[index] = 0;		
		set<int> tmpNeighbors = nodeNeighborsMap.at(*git);
		set<int>::iterator eit = tmpNeighbors.begin();
		while(eit!=tmpNeighbors.end()){
			if(*eit>*git){
				n_wait[index]++;
			}else{
				send_queue[index].insert(send_queue[index].end(), eit);
			}
			eit++;
		}

		if(n_wait[index]==0){
			color_queue.insert(color_queue.end(), *git);
		}

		git++;
	}

	ColorVerticesSequentially(&color_queue);
	//pack and send
	color_queue.clear();
	if(coloredNodes.size() < globalVertices.size()){
		
	}

	//color local vertices
	ColorVerticesSequentially(&localVertices);
	return 0;
}
