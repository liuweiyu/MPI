#include <map>
#include <set>
#include <unordered_set>
#include <vector>
#include <mpi.h>
#include <fstream>
#include <iostream>
#include <string>
#include <sstream>
#include <time.h>
using namespace std;

#define MASTER 0 

map<int, set<int>> nodeNeighborsMap;
int beginNode;
int endNode;
int* colors;
int* randoms;
set<int> globalVertices;
set<int> localVertices;
set<int> coloredNodes;

int* n_wait;
set<int>* send_queue;
set<int>* receive_queue;
set<int> send_thread;
set<int> receive_thread;
int* receive_threadCount;
int* send_threadCount;

int npes;
int tmpThreadID;

string Int2Str(int i){
	ostringstream s;
	s << i;
	return s.str();
}

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

void GetNodeNeighborInfo(string fileName){
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
				InsertIntoPartitionedNodeNeighborMap(node1, node2);
				InsertIntoPartitionedNodeNeighborMap(node2, node1);
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
		
		if(nodesCountMap->find(*it) == nodesCountMap->end()){			
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

int FindNexVertice(set<int>& nodes){
	int nextVertice = -1;
	int maxDegree = -1;
	set<int>::iterator it = nodes.begin();
	while(it != nodes.end()){
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
	set<int> neighbors = nodeNeighborsMap.at(vertice);
	set<int>::iterator it = neighbors.begin();
	set<int> tmpAllColors;
	while(it != neighbors.end()){
		int tmpColor = colors[*it-1];
		if(tmpColor!=-1){
			tmpAllColors.insert(tmpAllColors.end(), tmpColor);		
		}
		it++;
	}

	int color = 0;
	set<int>::iterator it2 = tmpAllColors.begin();
	while(it2 != tmpAllColors.end()){
		if(color == *it2){
			color++;
		}
		it2++;
	}

	return color;
}

void ColorVerticesSequentially(const set<int>* nodes){
	if(nodes->size() == 0){
		return;
	}

	set<int> tmpNodes = *nodes;
	while(tmpNodes.size() > 0){
		int nextVertice = FindNexVertice(tmpNodes);
		int color = GetColor(nextVertice);
		colors[nextVertice-1] = color;
		coloredNodes.insert(coloredNodes.end(), nextVertice);
		tmpNodes.erase(nextVertice);
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

	return nodeNum;
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

int PackAndSend(set<int>* color_queue, int* tmpAllThreadsColorQueueSize, int*& rbuf, int*& rdispls){
//	cout<<"begin pack and send"<<endl;
	int lengthPerThread = color_queue->size()*2;
	int* sbuf = new int[lengthPerThread*npes];
	for(int i = 0; i<npes; i++){
		int index = 0;
		set<int>::iterator it = color_queue->begin();
		while(it != color_queue->end()){		
			sbuf[i*lengthPerThread + index] = *it;
			sbuf[i*lengthPerThread + index + 1] = colors[*it-1];		
		
			index += 2;
			it++;
		}
	}

	int* scounts = new int[npes];
	for(int i = 0; i<npes; i++){
		scounts[i]=lengthPerThread;		
	}

	int* sdispls = new int[npes];	
	for(int i = 0; i<npes; i++){
		sdispls[i]=i*lengthPerThread;
	}

	int* rcounts = new int[npes];
	for(int i = 0; i<npes; i++){
		rcounts[i] = tmpAllThreadsColorQueueSize[i]*2;
	}
	
	rdispls[0] = 0;
	for(int i = 1; i<npes; i++){
		rdispls[i] = rdispls[i-1] + rcounts[i-1];
	}
	
	int length = rdispls[npes-1]+rcounts[npes-1];
	rbuf = (int*)realloc(rbuf, (length)*sizeof(int));

//	cout<<"before all to all"<<endl;
	MPI_Alltoallv(sbuf, scounts, sdispls, MPI_INT, rbuf, rcounts, rdispls, MPI_INT, MPI_COMM_WORLD);
//	cout<<"after all to all"<<endl;

	delete[] sbuf;
	sbuf = NULL;
	delete[] scounts;
	scounts = NULL;
	delete[] sdispls;
	sdispls = NULL;
	delete[] rcounts;
	rcounts = NULL;	

	return length;
}

int main(int argc, char** argv){	
	int totalNodeNum = 0;	
	int totalColoredNodeNum = 0;
	int totalGlobalNodeNum = 0;
	
	//initialize mpi
	MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &tmpThreadID);
    MPI_Comm_size(MPI_COMM_WORLD, &npes);

	//get node number and partition it equally by the number of threads
//	char* fileName = "E:\\2014 Fall\\Big Data Computer System\\Assignment1\\GraphColoring\\GraphColoring\\input\\le450_5a.col";
	char* fileName = "E:\\2014 Fall\\Big Data Computer System\\Assignment1\\GraphColoring\\GraphColoring\\input\\test.txt";
	totalNodeNum = GetNodeNum(fileName);
	cout<<"total node number is "<<totalNodeNum<<endl;

	int* sendbuf = (int*)malloc(npes*2*sizeof(int));
	if(tmpThreadID == MASTER){
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
	//send the start point and end point of nodes for each process
	MPI_Scatter(sendbuf, 2, MPI_INT, rbuf, 2, MPI_INT, MASTER, MPI_COMM_WORLD);
	delete[] sendbuf;
	sendbuf=NULL;

	beginNode = rbuf[0];
	endNode = rbuf[1];
	delete[] rbuf;
	rbuf = NULL;
	cout<<tmpThreadID<<": beginNode is "<<beginNode<<", endNode is "<<endNode<<endl;
	
	string tmp = "E:\\2014 Fall\\Big Data Computer System\\Assignment1\\GraphColoring\\GraphColoring\\tmp\\neighbors_" + Int2Str(tmpThreadID) + ".txt";
	ofstream tmpfile;
	tmpfile.open(tmp);

	//generate weights for all nodes in master node
	randoms = new int[totalNodeNum];
	int* scounts = new int[npes];
	int* displs = new int[npes];
	int rcount = endNode-beginNode+1;
	int* recvbuf = new int[rcount];
	if(tmpThreadID == MASTER){
		srand(time(NULL));
		unordered_set<int> rands;
		while(rands.size()<totalNodeNum){
			int tmpRand = rand()%totalNodeNum;
			rands.insert(rands.end(), tmpRand);
		}
		
		unordered_set<int>::iterator uit = rands.begin();
		int index = 0;
		while(uit!=rands.end()){
			randoms[index] = *uit;
			uit++;
			index++;
		}
		rands.clear();
	}else{
		for(int i = 0; i<totalNodeNum; i++){
			randoms[i]=0;
		}
	}

	//send weights to all processes
	MPI_Bcast(randoms, totalNodeNum, MPI_INT, MASTER, MPI_COMM_WORLD);

	tmpfile<<tmpThreadID<<", random numbers ";
	for(int i = 0; i<totalNodeNum; i++){
		tmpfile<<randoms[i]<<"\t";
	}
	tmpfile<<endl;
	
	//get graph info and store it into a hashmap
	GetNodeNeighborInfo(fileName);	
	
	tmpfile<<"neighbor info: ";
	map<int, set<int>>::iterator it = nodeNeighborsMap.begin();
	tmpfile<<": key\t neighbors"<<endl;
	while(it!=nodeNeighborsMap.end()){
		tmpfile<<it->first<<": ";
		set<int>::iterator it1 = it->second.begin();
		while(it1!=it->second.end()){
			tmpfile<<*it1<<"\t";
			it1++;
		}
		tmpfile<<endl;
		it++;
	}

	//get global vertices and local ones in current process
	GetGlobalAndLocalVertices();
	tmpfile<<"global vertices"<<endl;
	set<int>::iterator gvit = globalVertices.begin();
	while(gvit !=globalVertices.end()){
		tmpfile<<*gvit<<"\t";
		gvit++;
	}
	tmpfile<<endl;

	tmpfile<<"local vertices"<<endl;
	set<int>::iterator lvit = localVertices.begin();
	while(lvit!=localVertices.end()){
		tmpfile<<*lvit<<"\t";
		lvit++;
	}
	tmpfile<<endl;
	tmpfile.clear();
	tmpfile.close();

	int* agSbuf = new int[1];
	agSbuf[0] = globalVertices.size();
	int* agRbuf = new int[npes];
	MPI_Allgather(agSbuf, 1, MPI_INT, agRbuf, 1, MPI_INT, MPI_COMM_WORLD);

	for(int i = 0; i<npes; i++){
		totalGlobalNodeNum+=agRbuf[i];
	}
	cout<<tmpThreadID<<" total global vertices size is "<<totalGlobalNodeNum<<endl;

	//initialize color queue
	colors = new int[totalNodeNum];
	for(int i = 0; i<totalNodeNum; i++){
		colors[i] = -1;
	}
	
	//color global vertices	
	int globalVerticesCount = globalVertices.size();
	n_wait = new int[globalVerticesCount];
	send_queue = new set<int>[globalVerticesCount];
	receive_queue = new set<int>[globalVerticesCount];
	receive_threadCount = new int[npes];	
	send_threadCount = new int[npes];
	for(int i = 0; i<npes; i++){
		receive_threadCount[i]=0;
		send_threadCount[i]=0;
	}

	set<int> color_queue;
	set<int>::iterator git = globalVertices.begin();
	cout<<tmpThreadID<<", global Vertices size is "<<globalVerticesCount<<endl;
	int index=0;
	int nodeNumMean = totalNodeNum/npes;
	while(git != globalVertices.end()){
		cout<<*git<<endl;
		int gRandom = randoms[*git-1];
		n_wait[index] = 0;
		
		set<int> tmpNeighbors = nodeNeighborsMap.at(*git);
		set<int>::iterator eit = tmpNeighbors.begin();
		while(eit != tmpNeighbors.end()){
			if(*eit>=beginNode && *eit<=endNode){
				eit++;
				continue;
			}

			int tmpWeight = randoms[*eit-1];			
			if(tmpWeight > gRandom){
				n_wait[index]++;
			}

			eit++;			
		}

		if(n_wait[index]==0){
			color_queue.insert(color_queue.end(), *git);
		}

		git++;
		index++;
	}	

	cout<<tmpThreadID<<", globalVertices' size is "<<globalVerticesCount<<", n_wait is ";
	for(int i = 0; i<globalVerticesCount; i++){
		cout<<n_wait[i]<<",";
	}
	cout<<endl;
	
	ColorVerticesSequentially(&color_queue);
	set<int>::iterator cit;
	cit = color_queue.begin();
	cout<<tmpThreadID;
	while(cit!=color_queue.end()){
		cout<<", node "<<*cit<<", color is "<<colors[*cit-1]<<", ";
		cit++;
	}
	cout<<endl;

	int tmpColorQueueSize = color_queue.size();
	int* tmpAllThreadsColorQueueSize = new int[npes];
	MPI_Allgather(&tmpColorQueueSize, 1, MPI_INT, tmpAllThreadsColorQueueSize, 1, MPI_INT, MPI_COMM_WORLD);

	cout<<tmpThreadID<<"\t";
	for(int i = 0; i<npes; i++){
		totalColoredNodeNum += tmpAllThreadsColorQueueSize[i];
		cout<<tmpAllThreadsColorQueueSize[i]<<"\t";
	}
	cout<<endl;

	//pack and send
	int *buf = NULL;
	int *rdispls = new int[npes];
	int rBufLength = PackAndSend(&color_queue, tmpAllThreadsColorQueueSize, buf, rdispls);	
	color_queue.clear();
	
	while(totalColoredNodeNum < totalGlobalNodeNum){
		for(int i = 0; i<npes; i++){
			if(i==tmpThreadID){
				continue;
			}

			int endPoint;
			if(i<npes-1){
				endPoint = rdispls[i+1];
			}else{
				endPoint = rBufLength;
			}

			for(int j = rdispls[i]; j<endPoint;){
				int tmpNode = buf[j];
				int tmpColor = buf[j+1];
				colors[tmpNode-1] = tmpColor;
				
				cout<<tmpThreadID<<" get from thread "<<i<<", node "<<tmpNode<<", color "<<tmpColor<<endl;

				set<int> tmpNeighbors = nodeNeighborsMap.at(tmpNode);
				set<int>::iterator it = tmpNeighbors.begin();
				while(it != tmpNeighbors.end()){
					set<int>::iterator tmpIt = globalVertices.find(*it);
					//if not global vertice, skip
					if(tmpIt == globalVertices.end()){
						it++;
						continue;
					}

					//if weight is less than global vertice's weight or global certice has already been colored, skip
					if(randoms[*it-1]>randoms[tmpNode-1] || coloredNodes.find(*it)!=coloredNodes.end()){
						it++;
						continue;
					}
					
					//decrease the related value in n_wait by one
					int tmpIndex = distance(globalVertices.begin(), tmpIt);
					n_wait[tmpIndex]--;
					cout<<tmpThreadID<<" , tmpNeighbor "<<*it<<", tmpIndex "<<tmpIndex<<endl;
					if(n_wait[tmpIndex]==0){
						color_queue.insert(color_queue.end(), *it);
					}
					
					it++;
				}
				j+=2;
			}
		}

		cout<<tmpThreadID<<", n_wait ";
		for(int i = 0; i<globalVertices.size(); i++){
			cout<<n_wait[i]<<",";
		}
		cout<<endl;
		
		cout<<tmpThreadID<<", color_queue is:";
		set<int>::iterator cqit = color_queue.begin();
		while(cqit!=color_queue.end()){
			cout<<*cqit<<",";
			cqit++;
		}
		cout<<endl;

		ColorVerticesSequentially(&color_queue);
		set<int>::iterator cit2;
		cit2 = color_queue.begin();
		cout<<tmpThreadID<<" after sequential coloring ";
		while(cit2!=color_queue.end()){
			cout<<", node "<<*cit2<<", color is "<<colors[*cit2-1]<<", ";
			cit2++;
		}
		cout<<endl;

		tmpColorQueueSize = color_queue.size();
		MPI_Allgather(&tmpColorQueueSize, 1, MPI_INT, tmpAllThreadsColorQueueSize, 1, MPI_INT, MPI_COMM_WORLD);
		for(int i = 0; i<npes; i++){
			totalColoredNodeNum += tmpAllThreadsColorQueueSize[i];			
		}
		
		rBufLength = PackAndSend(&color_queue, tmpAllThreadsColorQueueSize, buf, rdispls);
		color_queue.clear();
	}

	delete[] rdispls;
	rdispls = NULL;
	delete[] buf;
	buf = NULL;
	delete[] tmpAllThreadsColorQueueSize;
	tmpAllThreadsColorQueueSize = NULL;
	delete[] n_wait;
	n_wait = NULL;
	delete[] send_queue;
	send_queue = NULL;
	delete[] receive_queue;
	receive_queue = NULL;

	//color local vertices
	ColorVerticesSequentially(&localVertices);

	string rltFileName = "E:\\2014 Fall\\Big Data Computer System\\Assignment1\\GraphColoring\\GraphColoring\\output\\result_" + Int2Str(tmpThreadID) + ".txt";
	ofstream outf;
	outf.open(rltFileName);
	if(!outf.is_open()){
		cout<<"error in opening file "<<rltFileName<<endl;
		return -1;
	}

	outf<<"nodeID\t color"<<endl;
	for(int i = beginNode; i<=endNode; i++){
		outf<<i<<"\t"<<colors[i-1]<<endl;
	}
	outf.clear();
	outf.close();

	MPI_Finalize();

	return 0;
}
