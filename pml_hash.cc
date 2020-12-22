#include "pml_hash.h"
/**
 * PMLHash::PMLHash 
 * 
 * @param  {char*} file_path : the file path of data file
 * if the data file exist, open it and recover the hash
 * if the data file does not exist, create it and initial the hash
 */
PMLHash::PMLHash(const char* file_path) {
    
    if ((this->start_addr = pmem_map_file(PATH, FILE_SIZE, PMEM_FILE_CREATE, 0666, &this->mapped_len, &this->is_pmem)) == NULL) {
        perror("pmem_map_file");
        exit(1);
    }//打开NVM存储空间；
    this->meta = (metadata*)this->start_addr;//meta起始位置
    this->meta->size= 16;
    this->meta->level= 16;
    this->meta->next=0;
    this->meta->overflow_num=0;
    //元数据初始化
    this->table_arr = (pm_table*)(this->meta + sizeof(metadata));//pm_table起始位置
    this->overflow_addr = this->meta + FILE_SIZE / 2; //29959

    std::ifstream ifile;
    if(ifile.is_open()){
        ifile.open(file_path);
        uint64_t i;
        string k;
        while(1){
            ifile>>k>>i;
            if(k=="INSERT"){
                this->insert(i,i);
            }
            else if(k=="READ"){
                this->search(i,i);
            }
            if(ifile.eof()!=0) break;
        }
        ifile.close();
    }

    /*
    ifstream ifile;
    ifile.open(file_path);
    long long i;
    pm_table->fill_num=0;
    string k;
    while(1){
        ifile>>k>>i;
        pm_table->kv_arr[fill_num]=i;
        fill_num++;
        if(ifile.eof()!=0) break;
    }
    ifile.close();
    */
    //vector<int> *p[12]=NULL;

}
/**
 * PMLHash::~PMLHash 
 * 
 * unmap and close the data file
 */
PMLHash::~PMLHash() {
    pmem_persist(this->start_addr,this->mapped_len);
    pmem_unmap(this->start_addr, FILE_SIZE);
}
/**
 * PMLHash 
 * 
 * split the hash table indexed by the meta->next
 * update the metadata
 */
void PMLHash::split() {
    //调取table
    uint64_t next0 = this->meta->next;//记录next 避免反复调用
    int temp_f_n = this->table_arr[next0].fill_num;//临时变量用于记录一共有多少变量需要分割
    int temp_h;//用于记录哈希值
    uint64_t list = next0;//指向读取位置
    uint64_t list1 = next0, list2 = next0+this->meta->level;//指向写入位置(桶）
    int j = 0;//指向分裂桶内部的读取数据
    this->table_arr[list].fill_num = 0;
    int l1 = 0, l2 = 0;//桶内部的写入数组
    for (int i = 0; i < temp_f_n; i++)
    {
        if (j == HASH_SIZE)
        {
            j = 0;
            list = this->table_arr[list].next_offset;
            this->table_arr[list].fill_num = 0;
        }//overflow_charge & enter
        temp_h = hashFunc(this->table_arr[list].kv_arr[j].key, this->meta->level * 2);
        if (temp_h = next0)
        {
            if (l1 == HASH_SIZE)
            {
                l1 = 0;
                list1 = this->table_arr[list1].next_offset;
            }
            this->table_arr[list1].kv_arr[l1] = this->table_arr[list].kv_arr[j];
            l1++;
            this->table_arr[list1].fill_num ++;
            if (list1 != next0)
                this->table_arr[next0].fill_num++;
        }
        else
        {
            if (l2 == HASH_SIZE)
            {
                l2 = 0;
                list2 = newOverflowTable(this->table_arr[list2].next_offset);
//变为下一个页面序号，并且为上一桶的nextoffset赋值
            }
            this->table_arr[list2].kv_arr[l2] = this->table_arr[list].kv_arr[j];
            l2++;
            this->table_arr[list2].fill_num++;
            if(list2!=next0 + this->meta->level)
                this->table_arr[next0 + this->meta->level].fill_num++;
        }
        
    }
    // fill the split table
    // fill the new table
    recover_all(list1);//回收分裂桶空出来的页面
    // update the next of metadata
    this->meta->next++;
    this->meta->size++;
    if (this->meta->next >= this->meta->level)
    {
        this->meta->level *= 2;
        this->meta->next = 0;
    }
    /*if(this->meta->level<100)
    {
        cout<<"split--"<<this->meta->next-1<<endl;
    }*/
}
/**
 * PMLHash 
 * 
 * @param  {uint64_t} key     : key
 * @param  {size_t} hash_size : the N in hash func: idx = hash % N
 * @return {uint64_t}         : index of hash table array
 * 
 * need to hash the key with proper hash function first
 * then calculate the index by N module
 */
uint64_t PMLHash::hashFunc(const uint64_t &key, const size_t &hash_size) {
    int temp;
    //temp = key % 4;
    return key % hash_size;
}

/** 
 * PMLHash 
 * 
 * @param  {uint64_t} offset : the file address offset of the overflow hash table
 *                             to the start of the whole file
 * @return {pm_table*}       : the virtual address of new overflow hash table
 */
uint64_t PMLHash::newOverflowTable(uint64_t &offset) {
    if (this->Reuse.empty() == true)
    {
        offset = this->meta->overflow_num + HALF_SIZE;
        this->meta->overflow_num++;
        return this->meta->overflow_num - 1 + HALF_SIZE;
    }
    offset = this->Reuse.top();
    this->Reuse.pop();
    return offset;
}

/**
 * PMLHash 
 * 
 * @param  {uint64_t} key   : inserted key
 * @param  {uint64_t} value : inserted value
 * @return {int}            : success: 0. fail: -1
 * 
 * insert the new kv pair in the hash
 * 
 * always insert the entry in the first empty slot
 * 
 * if the hash table is full then split is triggered
 */
int PMLHash::insert(const uint64_t &key, const uint64_t &value) {
    uint64_t h0 = key % this->meta->level;
    entry temp;
    temp.key = key;
    temp.value = value;
    if (h0 < this->meta->next){
        h0 = key % (2 * this->meta->level);
    }
    uint64_t a1 = this->table_arr[h0].fill_num;
    if(this->table_arr[h0].fill_num < HASH_SIZE)
    {
        this->table_arr[h0].kv_arr[this->table_arr[h0].fill_num] = temp;
    }
    else if(this->table_arr[h0].fill_num % HASH_SIZE == 0)
    {
        uint64_t then = h0;
        while (this->table_arr[then].next_offset != 0)
            then = this->table_arr[then].next_offset;
        then = this->newOverflowTable(this->table_arr[then].next_offset);
        this->table_arr[then].kv_arr[0] = temp;
        this->table_arr[then].fill_num++;
        split();
    }
    else
    {
        uint64_t then = h0;
        while (this->table_arr[then].next_offset != 0)
            then = this->table_arr[then].next_offset;
        this->table_arr[then].kv_arr[this->table_arr[then].fill_num] = temp;
        this->table_arr[then].fill_num++;
        split();
    }
    /*if(this->meta->level<100)
    {
        cout<<"insert--"<<key<<endl;
    }*/
    return 0;
}
/**
 * PMLHash 
 * 
 * @param  {uint64_t} key   : the searched key
 * @param  {uint64_t} value : return value if found
 * @return {int}            : 0 found, -1 not found
 * 
 * search the target entry and return the value
 */
int PMLHash::search(const uint64_t &key, uint64_t &value) {
    uint64_t temp;
    temp = hashFunc(key, this->meta->level);
    if(temp<this->meta->next){
        temp=hashFunc(key, (this->meta->level)*2);
    }
    int i=0;
    int list = 0;
    uint64_t Num = this->table_arr[temp].fill_num;
    while (i < Num){
        list = i % 16;
        if( this->table_arr[temp].kv_arr[list].key == key){
            value = this->table_arr[temp].kv_arr[list].value;
            return 0;
        }
        i++;
        if(list==0&&i!=0){
            temp = this->table_arr[temp].next_offset;
        }
    }
    return -1;
}

/**
 * PMLHash 
 * 
 * @param  {uint64_t} key : target key
 * @return {int}          : success: 0. fail: -1
 * 
 * remove the target entry, move entries after forward
 * if the overflow table is empty, remove it from hash
 */
int PMLHash::remove(const uint64_t &key) {
    uint64_t temp ,tb;
    int flag = -1;
    temp = hashFunc(key, this->meta->level);
    if (temp < this->meta->next) {
        temp = hashFunc(key, (this->meta->level) * 2);
    }
    uint64_t H = temp;
    int i = 0;
    int list = 0;
    uint64_t Num = this->table_arr[temp].fill_num;
    entry* space_place;
    while (i < Num) {
        list = i % 16;
        if (this->table_arr[temp].kv_arr[list].key == key) {
            
            flag = 0;
            this->table_arr[H].fill_num--;
            space_place = &this->table_arr[temp].kv_arr[list];
        }
        i++;
        if (list == 0 && i != 0 && this->table_arr[temp].next_offset !=0 ) {
            tb = temp;
            temp = this->table_arr[temp].next_offset;
        }
    }
    if (flag == 0)
    {
        *space_place = this->table_arr[temp].kv_arr[this->table_arr[temp].fill_num - 1];
        this->table_arr[temp].fill_num--;
        if (this->table_arr[temp].fill_num == 0 && this->table_arr[H].fill_num!=0)
        {
            this->recover_one(tb);
        }
    }
    return flag;

}

/**
 * PMLHash 
 * 
 * @param  {uint64_t} key   : target key
 * @param  {uint64_t} value : new value
 * @return {int}            : success: 0. fail: -1
 * 
 * update an existing entry
 */
int PMLHash::update(const uint64_t &key, const uint64_t &value) {
    uint64_t temp;
    temp = hashFunc(key, this->meta->level);
    if (temp < this->meta->next) {
        temp = hashFunc(key, (this->meta->level) * 2);
    }
    int i = 0;
    int list = 0;
    uint64_t Num = this->table_arr[temp].fill_num;
    while (i < Num) {
        list = i % 16;
        if (this->table_arr[temp].kv_arr[list].key == key) {
            this->table_arr[temp].kv_arr[list].value = value;
            return 0;
        }
        i++;
        if (list == 0 && i != 0) {
            temp = this->table_arr[temp].next_offset;
        }
    }
    return -1;


}


int PMLHash::recover_all(uint64_t &offset)
{
    if (offset<HALF_SIZE || offset>HALF_SIZE * 2)
        return 1;
    uint64_t temp = offset;
    while (temp != 0)
    {
        offset = 0;
        this->Reuse.push(temp);
        this->table_arr[temp].fill_num = 0;
        temp = this->table_arr[temp].next_offset;
        this->table_arr[temp].next_offset = 0;
    }
    return 0;
}

int PMLHash::recover_one(uint64_t &offset)
{
    if (offset<HALF_SIZE || offset>HALF_SIZE * 2)
        return 1;
    uint64_t temp = offset;
    offset = 0;
    this->Reuse.push(temp);
    this->table_arr[temp].fill_num = 0;
    return 0;
}


/*
//建立空栈 
stack::stack(){
	Node *head=new Node;
	length=0;
}
//构造：复制数组e[]的前n个元素
stack::stack(const int * e, const int n){
	length=n;
	int i;
	Node *temp=new Node;
	head=temp;
	
	while(i<n){
		
	}
}
// 复制构造
stack::stack(const stack& ll){
	Node *temp=new Node;
	head=temp;
	Node *obj=ll.head;
	while(obj!=NULL){
		temp->val=obj->val;
		temp->next=new Node;
	}
	length=ll.length;
}
//析构函数 
stack::~stack(){
	while(length!=0){
		this->pop();
		length--;
	}
}
//判断栈空
bool stack::empty(){
	if(length==0)return true;
	return false;
}
//返回元素数目
int stack::size(){
	return length;
}
//返回栈顶元素 
int stack::top(){
	int res;
	Node *obj=head;
	while(obj->next!=NULL){
		obj=obj->next;
	}
	res=obj->val;
	return res;
}
//弹出栈顶元素
void stack::pop(){
	Node *temp=new Node;
	Node *obj=head;
	while(obj->next!=NULL){
		temp=obj;
		obj=obj->next;
	}
	temp->next=NULL;
	delete(obj);
	length--;
}
//压入元素
void stack::push(int e){
	Node *temp=new Node;
	temp->next=NULL;
	temp->val=e;
	Node *obj=head;
	while(obj->next!=NULL){
		obj=obj->next;
	}
	obj->next=temp;
	length++;
}
*/


/*			new
#include "pml_hash.h"
/**
 * PMLHash::PMLHash 
 * 
 * @param  {char*} file_path : the file path of data file
 * if the data file exist, open it and recover the hash
 * if the data file does not exist, create it and initial the hash
 */


/*			new
PMLHash::PMLHash(const char* file_path) {
    //meta
    meta->size= 16;
    meta->level= 16;
    meta->next=0;
    meta->overflow_num=0;
    //pm_tablr
    table_arr->fill_num=0;
    table_arr->next_offset=0;

    start_addr = new pm_table[256*256];
    //stack
    //stack sta;
    sta.length=0;
    sta.head=overflow_addr;
  
    ifstream ifile;
    if(ifile.is_open()){
        ifile.open(file_path);
        long long i;
        string k;
        while(1){
            ifile>>k>>i;
            if(k=="INSERT"){
                insert(i,i);
            }
            else if(k=="READ"){
                search(i,i);
            }
            fill_num++;
            if(ifile.eof()!=0) break;
        }
        ifile.close();
    }
    else{
        
    }
    /*
    ifstream ifile;
    ifile.open(file_path);
    long long i;
    pm_table->fill_num=0;
    string k;
    while(1){
        ifile>>k>>i;
        pm_table->kv_arr[fill_num]=i;
        fill_num++;
        if(ifile.eof()!=0) break;
    }
    ifile.close();
    */
    //vector<int> *p[12]=NULL;




/*			new
    return 0;

}
/**
 * PMLHash::~PMLHash 
 * 
 * unmap and close the data file
 */



/*			new
PMLHash::~PMLHash() {
    pmem_unmap(start_addr, FILE_SIZE);
}
/**
 * PMLHash 
 * 
 * split the hash table indexed by the meta->next
 * update the metadata
 */



/*			new
void PMLHash::split() {
    // fill the split table

    // fill the new table

    // update the next of metadata
    
}
/**
 * PMLHash 
 * 
 * @param  {uint64_t} key     : key
 * @param  {size_t} hash_size : the N in hash func: idx = hash % N
 * @return {uint64_t}         : index of hash table array
 * 
 * need to hash the key with proper hash function first
 * then calculate the index by N module
 */



/*			new
uint64_t PMLHash::hashFunc(const uint64_t &key, const size_t &hash_size) {
    int temp;
    //temp = key % 4;
    return key % hash_size;
}

/** 
 * PMLHash 
 * 
 * @param  {uint64_t} offset : the file address offset of the overflow hash table
 *                             to the start of the whole file
 * @return {pm_table*}       : the virtual address of new overflow hash table
 */



/*			new
pm_table* PMLHash::newOverflowTable(uint64_t &offset) {



    return ;
}

/**
 * PMLHash 
 * 
 * @param  {uint64_t} key   : inserted key
 * @param  {uint64_t} value : inserted value
 * @return {int}            : success: 0. fail: -1
 * 
 * insert the new kv pair in the hash
 * 
 * always insert the entry in the first empty slot
 * 
 * if the hash table is full then split is triggered
 */




/*			new
int PMLHash::insert(const uint64_t &key, const uint64_t &value) {
    /*
    int index;
    entryn num;
    index = hashFunc(key, meta->level);
    if(temp < overflow_num){
        index = hashFunc(key , meta->level+1);
    }
    num.num.key=key;
    num.num.value=value;
    num.next=NUll;

    if( < ){

    }
    else{

        split();
    }
    
    */

//}

/**
 * PMLHash 
 * 
 * @param  {uint64_t} key   : the searched key
 * @param  {uint64_t} value : return value if found
 * @return {int}            : 0 found, -1 not found
 * 
 * search the target entry and return the value
 */



/*			new
int PMLHash::search(const uint64_t &key, uint64_t &value) {
    int temp;
    void *start;
    start=start_addr;
    temp = hashFunc(key, level);
    if(temp<next){
        temp=hashFunc(key, level*2);
    }
    int i=0;
    while (i < start_addr[temp].fill_num){
        if( start_addr[temp].kv_arr[i].key == key){
            value = start_addr[temp].kv_arr[i].value;
            return value;
        }
        i++;
        if(i==16){
            

            break;
        }
    }
    return -1;

}

/**
 * PMLHash 
 * 
 * @param  {uint64_t} key : target key
 * @return {int}          : success: 0. fail: -1
 * 
 * remove the target entry, move entries after forward
 * if the overflow table is empty, remove it from hash
 */



/*			new
int PMLHash::remove(const uint64_t &key) {
    int temp;
    temp=hashFunc(key,level);
    int fi=search(key);
    if(fi == -1){
        return -1;
    }
    else{
        int temp;
        void *start;
        start=start_addr;
        temp = hashFunc(key, level);
        if(temp<next){
            temp=hashFunc(key, level*2);
        }
        int i=0;
        while (i < start_addr[temp].fill_num){
            if( start_addr[temp].kv_arr[i].key == key){
                start_addr[temp].kv_arr[i].value = start_addr[temp].kv_arr[fill_num-1].value;
                start_addr[temp].kv_arr[i].key = start_addr[temp].kv_arr[fill_num-1].key;
                start_addr[temp].fill_num -= 1;

                int k;
                if(start_addr[temp].fill_num == 0){
                    if(temp<level){
                        k = temp - level;
                    }
                    else{
                        k = temp - level/2;
                    }
                    sta.push(temp);
                }
                return 0;
            }
            i++;
        }
        return -1;
    }

}

/**
 * PMLHash 
 * 
 * @param  {uint64_t} key   : target key
 * @param  {uint64_t} value : new value
 * @return {int}            : success: 0. fail: -1
 * 
 * update an existing entry
 */



/*			new
int PMLHash::update(const uint64_t &key, const uint64_t &value) {
    int fi=search(key);
    if(fi == -1){
        return -1;
    }
    else{
        int temp;
        void *start;
        start=start_addr;
        temp = hashFunc(key, level);
        if(temp<next){
            temp=hashFunc(key, level*2);
        }
        int i=0;
        while (i < start_addr[temp].fill_num){
            if( start_addr[temp].kv_arr[i].key == key){
                start_addr[temp].kv_arr[i].value = value;
                return 0;
            }
            i++;
            if(i==16){


                break;
            }
        }
        return -1;
    }


}



/*
//建立空栈 
stack::stack(){
	Node *head=new Node;
	length=0;
}
//构造：复制数组e[]的前n个元素
stack::stack(const int * e, const int n){
	length=n;
	int i;
	Node *temp=new Node;
	head=temp;
	
	while(i<n){
		
	}
}
// 复制构造
stack::stack(const stack& ll){
	Node *temp=new Node;
	head=temp;
	Node *obj=ll.head;
	while(obj!=NULL){
		temp->val=obj->val;
		temp->next=new Node;
	}
	length=ll.length;
}
//析构函数 
stack::~stack(){
	while(length!=0){
		this->pop();
		length--;
	}
}
//判断栈空
bool stack::empty(){
	if(length==0)return true;
	return false;
}
//返回元素数目
int stack::size(){
	return length;
}
//返回栈顶元素 
int stack::top(){
	int res;
	Node *obj=head;
	while(obj->next!=NULL){
		obj=obj->next;
	}
	res=obj->val;
	return res;
}
//弹出栈顶元素
void stack::pop(){
	Node *temp=new Node;
	Node *obj=head;
	while(obj->next!=NULL){
		temp=obj;
		obj=obj->next;
	}
	temp->next=NULL;
	delete(obj);
	length--;
}
//压入元素
void stack::push(int e){
	Node *temp=new Node;
	temp->next=NULL;
	temp->val=e;
	Node *obj=head;
	while(obj->next!=NULL){
		obj=obj->next;
	}
	obj->next=temp;
	length++;
}
*/