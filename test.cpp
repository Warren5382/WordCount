#include <boost/unordered_map.hpp>
#include <google/dense_hash_map>
#include <stdio.h>
#include <iostream>
#include <pthread.h>
#include <assert.h>
//#include <string.h>
#define THREADNUM 4
#define FILESIZE 22711454L
//#define FILESIZE 249825962L
#define ONCESIZE (1024*20)
//#define MyMap boost::unordered_map<std::string, long>
#define MyMap google::dense_hash_map<std::string, long>
#define judgeletter_bit(ch) (((ch)>>6)==1)&&((((ch)-1)&31)<26)
#define judgeletter_table(ch) (*(table + (ch)))

MyMap WordMap[THREADNUM];

void process(char* buffer, long length, long oncesize, char* word, int& index, MyMap& wordmap) {
	int i = 0;
	int j = index;
	long minlength = oncesize;
	if(length-oncesize < 0) {
		minlength = length;
	}
	for(i = 0; i < minlength; i++) {
		/*
		switch(judgeletter_table(buffer[i])) {
			case 1:
				word[j] = buffer[i]+32;
				j++;
				break;
			case 2:
				word[j] = buffer[i];
				j++;
				break;
			default:
				if(j > 0) {
					word[j] = '\0';
					++wordmap[word];
					j=0;
				}
				break;
		}
		*/
		
		if(judgeletter_bit(buffer[i])) {
			if(buffer[i] >= 'a') {
				word[j] = buffer[i];
				j++;
			}
			else {
				word[j] = buffer[i]+32;
				j++;
			}
		}
		else {
			if(j > 0) {
				word[j] = '\0';
				if(j<7) ++wordmap[word];
				j=0;
			}
		}
	}
	for(; i < length; i++) {
		/*switch(judgeletter_table(buffer[i])) {
			case 1:
				word[j] = buffer[i]+32;
				j++;
				break;
			case 2:
				word[j] = buffer[i];
				j++;
				break;
			default:
				if(j > 0) {
					word[j] = '\0';
					++wordmap[word];
					j=0;
				}
				break;
		}
		*/
		
		if(judgeletter_bit(buffer[i])) {
			if(buffer[i] >= 'a') {
				word[j] = buffer[i];
				j++;
			}
			else {
				word[j] = buffer[i]+32;
				j++;
			}
		}
		else {
			if(j > 0) {
				word[j] = '\0';
				if(j<7) ++wordmap[word];
				j=0;
			}
		}
	}
	index = j;
};

typedef struct {
	std::string str;
	long count;
} wordcount;

void heapfy(wordcount* A, int i, int heap_size) {
	int l, r, largest;
	wordcount temp;
	l = 2*i;
	r = 2*i + 1;
	if(l <= heap_size && A[l-1].count > A[i-1].count)
		largest = l;
	else 
		largest = i;
	if(r <= heap_size && A[r-1].count > A[largest-1].count)
		largest = r;
	if(largest != i) {
		temp = A[largest-1];
		A[largest-1] = A[i-1];
		A[i-1] = temp;
		heapfy(A, largest, heap_size);
	}
};

void buildheap(wordcount* A, int length) {
	int i = 0;
	for(i = length/2; i > 0; i--)
		heapfy(A, i, length);
};

void TopK(int K, wordcount* A, int length) {
	buildheap(A, length);
	wordcount temp;
	int j = 0;
	while(1) {
		temp = A[0];
		A[0] = A[length-1];
		A[length-1] = temp;
		length--;
		j++;
		if(j == K) break;
		heapfy(A, 1, length);
	}
};

bool IsResult(std::string& str) {
	if(str.length() > 4) return true;
	if(str == "the") return false;
	if(str == "and") return false;
	if(str == "i") return false;
	if(str == "to") return false;
	if(str == "of") return false;
	if(str == "a") return false;
	if(str == "in") return false;
	if(str == "was") return false;
	if(str == "that") return false;
	if(str == "had") return false;
	if(str == "he") return false;
	if(str == "you") return false;
	if(str == "his") return false;
	if(str == "my") return false;
	if(str == "it") return false;
	if(str == "as") return false;
	if(str == "with") return false;
	if(str == "her") return false;
	if(str == "for") return false;
	if(str == "on") return false;
	return true;
};

void outputresult(wordcount* A, int K, int size) {
	int i = 0;
	while(1) {
		size--;
		if(IsResult(A[size].str)){
			std::cout << "string " << A[size].str << " count " << A[size].count << std::endl;
			i++;
		}
		if(i == K) break;
	}
};

typedef struct {
	int id;
	char* buffer;
} Parm;

void* worker(void* arg) {
	Parm* parm = (Parm*)arg;
	int id = parm->id;
	char* buffer = parm->buffer;
	FILE* fp;
	fp = fopen("document.txt", "r");
	long TotalSize = FILESIZE/THREADNUM;
	if(id == THREADNUM-1) {
		TotalSize += FILESIZE%THREADNUM;
	}
	long OnceSize = ONCESIZE;
	fseek(fp, id*TotalSize, SEEK_SET);
	if(fp == NULL) {
		printf("cannot open the file\n");
		return (void*)0;
	}
	int ret = 0;
	long loop = TotalSize/OnceSize;
	char word[30];
	int index = 0;
	long suffix = TotalSize%OnceSize;
	for(int i = 0; i < loop; i++) {
		ret = fread(buffer, sizeof(char), OnceSize, fp);
		process(buffer, ret, OnceSize, word, index, WordMap[id]);
	}
	if(suffix > 0) {
		ret = fread(buffer, sizeof(char), suffix+30, fp);
		process(buffer, suffix, ret, word, index, WordMap[id]);
	}
	//printf("***%d*****\n", WordMap[id].size());
	fclose(fp);
	return (void*)0;
};

void merge() {
	MyMap::iterator mapbegin;
	MyMap::iterator mapend;
	for(int i = 1; i < THREADNUM; i++) {
		mapbegin = WordMap[i].begin();
		mapend = WordMap[i].end();
		for(; mapbegin != mapend; mapbegin++) {
			WordMap[0][mapbegin->first] += mapbegin->second;
		}
	}
}

/*
void SetPolicy(pthread_t& id, pthread_attr_t& thread_attr, int cpu) {
	int err;
	err = pthread_attr_init(&thread_attr);
	assert(err == 0);

	err = pthread_attr_setschedpolicy(&thread_attr, SCHED_RR);
	assert(err == 0);
	
	cpu_set_t mask;
	CPU_ZERO(&mask);
	CPU_SET(cpu, &mask);
	if (pthread_setaffinity_np(id, sizeof(mask), &mask) < 0) {
		printf("set thread affinity failed\n");
	}
};
*/

int main(void) {
	/*
	for(int i = 0; i < 256; i++) {
		table[i] = 0;
	}
	for(int i = 65; i <= 97 ; i++) {
		*(table + i) = 1;
		*(table + i + 32) = 2;
	}
	*/
	for(int i = 0; i < THREADNUM; i++) {
		WordMap[i].set_empty_key("");
	}
	pthread_t id[THREADNUM];
	//pthread_attr_t thread_attr[THREADNUM];
	Parm parm[THREADNUM];
	char* Buffer = new char[(ONCESIZE+30)*THREADNUM];
	//char* Buffer = (char *)malloc(sizeof(char)*((ONCESIZE+30)*THREADNUM));
	if(Buffer == NULL) {
		return 0;
	}
	for(int i = 0; i < THREADNUM; i++) {
		parm[i].id = i;
		parm[i].buffer = Buffer + i*(ONCESIZE+30);
		//SetPolicy(id[i], thread_attr[i], i);
		pthread_create(&id[i], NULL, worker, (void*)(&parm[i]));
	};
	for(int i = 0; i < THREADNUM; i++) {
		pthread_join(id[i], NULL);
	}
	merge();
	//printf("*****total word size%d*********\n", WordMap[0].size());

	wordcount* WordHeap = new wordcount[WordMap[0].size()];
	//wordcount* WordHeap = (wordcount*)malloc(sizeof(wordcount)*WordMap[0].size());
	MyMap::iterator mapbegin = WordMap[0].begin();
	MyMap::iterator mapend = WordMap[0].end();
	int count = 0;
	for(; mapbegin != mapend; mapbegin++) {
		WordHeap[count].str = mapbegin->first;
		WordHeap[count].count = mapbegin->second;
		count++;
	}

	TopK(30, WordHeap, count);
	outputresult(WordHeap, 10, count);

	delete[] WordHeap;
	//free(WordHeap);
	delete[] Buffer;
	//free(Buffer);
	for(int i = 0; i < THREADNUM; i++) {
		WordMap[i].clear();
	}
	return 0;
}
