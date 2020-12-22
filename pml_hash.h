#include <libpmem.h>
#include <stdint.h>
#include <iostream>
#include <fstream>
#include <unistd.h>
#include <memory.h>
#include <vector>
#include<sys/types.h>
#include<sys/stat.h>
#include<fcntl.h>
#include<stdio.h>
#include<errno.h>
#include<stdlib.h>
#include<stack>

#define TABLE_SIZE 16 // adjustable
#define HASH_SIZE  16 // adjustable
#define FILE_SIZE 1024 * 1024 * 16 // 16 MB adjustable
#define HALF_SIZE 30840 //overflow list
#define PATH  "/dev/peme3"

using namespace std;
typedef struct metadata {
    size_t size;            // the size of whole hash table array 整个哈希数组大小 TABLE_SIZE
    uint64_t level;           // level of hash 哈希级数 初始化为 0
    uint64_t next;          // the index of the next split hash table 分裂点序号 0
    uint64_t overflow_num;  // amount of overflow hash tables  溢出hashtable数量 0
} metadata;//元数据

// data entry of hash table
typedef struct entry {
    uint64_t key;
    uint64_t value;
} entry;//键值对结构

/*
typedef struct entryn{
    entry num;
    uint64_t *next; 
}entryn;
*/


// hash table
typedef struct pm_table {
    entry kv_arr[TABLE_SIZE];   // data entry array of hash table 键值对数组 大小为hash_size
    uint64_t fill_num = 0;          // amount of occupied slots in kv_arr KV_ARR中占用的插槽数量 0
    uint64_t next_offset;       // the file address of overflow hash table 链接下一个溢出页面
} pm_table;

// persistent memory linear hash持久内存线性哈希
class PMLHash {
private:
    void* start_addr;      // the start address of mapped file
    void* overflow_addr;   // the start address of overflow table array
    metadata* meta;        // virtual address of metadata
    pm_table* table_arr;   // virtual address of hash table array
    void split();//分裂函数
    uint64_t hashFunc(const uint64_t &key, const size_t &hash_size);//利用key%hash_size寻址
    uint64_t newOverflowTable(uint64_t &offset);
    stack<uint64_t>Reuse;

public:
    PMLHash() = delete;
    PMLHash(const char* file_path);
    ~PMLHash();
    int insert(const uint64_t &key, const uint64_t &value);
    int search(const uint64_t &key, uint64_t &value);
    int remove(const uint64_t &key);
    int update(const uint64_t &key, const uint64_t &value);
    //系统空间回收（某点之后连续所有溢出页面 不包括该点）
    int recover_all(uint64_t &offset);
    //系统空间回收（回收一个溢出页面 在offset之后）
    int recover_one(uint64_t &offset);
	size_t mapped_len;
	int is_pmem;
};
/*
//栈 
struct Node{
	int val;
	Node *next;
	Node(){ 
		val = -1; 
		next = NULL; 
	}
	Node(int e, Node *node = NULL){
		val = e;
		next = node;
	}
};

class stack{	
public:
	int length;//长度
	Node *head;//栈底 
	//建立空栈 
	stack();
	//构造：复制数组e[]的前n个元素
	stack(const int * e, const int n);
	// 复制构造
	stack(const stack& ll); 
	//析构函数 
	~stack();
	//判断栈空
	bool empty();
	//返回元素数目
	int size(); 
	//返回栈顶元素 
	int top();
	//弹出栈顶元素
	void pop();
	//压入元素
	void push(int e); 
};
*/





/*			new
#include <libpmem.h>
#include <stdint.h>
#include <iostream>
#include <string.h>
#include <unistd.h>
#include <memory.h>
#include <vector>

#define TABLE_SIZE 16 // adjustable
#define HASH_SIZE  16 // adjustable
#define FILE_SIZE 1024 * 1024 * 16 // 16 MB adjustable

using namespace std;

typedef struct metadata {
    size_t size;            // the size of whole hash table array s-16
    size_t level;           // level of hash  s-16
    uint64_t next;          // the index of the next split hash table s-0
    uint64_t overflow_num;  // amount of overflow hash tables s-0
} metadata;

// data entry of hash table
typedef struct entry {
    uint64_t key;
    uint64_t value;
} entry;
/*
typedef struct entryn{
    entry num;
    uint64_t *next; 
}entryn;
*/



/*			new
// hash table
typedef struct pm_table {
    entry kv_arr[TABLE_SIZE];   // data entry array of hash table
    uint64_t fill_num;          // amount of occupied slots in kv_arr s-0
    uint64_t next_offset;       // the file address of overflow hash table s- -1
} pm_table;

// persistent memory linear hash
class PMLHash {
private:
    void* start_addr;      // the start address of mapped file
    void* overflow_addr;   // the start address of overflow table array
    metadata* meta;        // virtual address of metadata
    pm_table* table_arr;   // virtual address of hash table array

	//stack *sta;				// the stack for reuse

    void split();
    uint64_t hashFunc(const uint64_t &key, const size_t &hash_size);
    pm_table* newOverflowTable(uint64_t &offset);

public:
    PMLHash() = delete;
    PMLHash(const char* file_path);
    ~PMLHash();

    int insert(const uint64_t &key, const uint64_t &value);
    int search(const uint64_t &key, uint64_t &value);
    int remove(const uint64_t &key);
    int update(const uint64_t &key, const uint64_t &value);
};
/*
//栈 
struct Node{
	int val;
	Node *next;
	Node(){ 
		val = -1; 
		next = NULL; 
	}
	Node(int e, Node *node = NULL){
		val = e;
		next = node;
	}
};

class stack{	
public:
	int length;//长度
	Node *head;//栈底 
	//建立空栈 
	stack();
	//构造：复制数组e[]的前n个元素
	stack(const int * e, const int n);
	// 复制构造
	stack(const stack& ll); 
	//析构函数 
	~stack();
	//判断栈空
	bool empty();
	//返回元素数目
	int size(); 
	//返回栈顶元素 
	int top();
	//弹出栈顶元素
	void pop();
	//压入元素
	void push(int e); 
};
*/