#ifndef __mapreduce_h__
#define __mapreduce_h__

// Different function pointer types used by MR
typedef char *(*Getter)(char *key, int partition_number);
typedef void (*Mapper)(char *file_name, int partition);
typedef void (*Reducer)(char *key, Getter get_func, int partition_number);
typedef unsigned long (*Partitioner)(char *key, int num_partitions);

// External functions: these are what you must define
void MR_Emit(char *key, char *value, int partition);

unsigned long MR_DefaultHashPartition(char *key, int num_partitions);

void MR_Run(int argc, char *argv[],
            Mapper map, int num_mappers,
            Reducer reduce, int num_reducers,
            Partitioner partition);

/*--------------------------------------------------------------
----------------------------------------------------------------
----------------------------------------------------------------
----------------------------------------------------------------
----------------------------------------------------------------*/

#include <stdbool.h>
/**
 * @brief 系统调用宏，获取线程的tid
 *
 */
#ifndef SYS_gettid
// i386: 224, ia64: 1105, amd64: 186, sparc 143
#ifdef __ia64__
#define SYS_gettid 1105
#elif __i386__
#define SYS_gettid 224
#elif __amd64__
#define SYS_gettid 186
#elif __sparc__
#define SYS_gettid 143
#else
#error define gettid for the arch
#endif
#endif

/**
 * @brief 增加的struct 执行任务和管理kv对
 */

typedef struct
    my_map_task
{
    char *input;    //输入待处理文件
    int state;      //任务状态
    int pthread_id; //处理该任务的线程id
    int task_id;    //任务id
    char **mediate; //处理文件得到的中间文件
} task;

typedef struct
    my_key_value
{
    char key[40];
    char value[8];
} kv;
typedef struct
    my_kv_array
{
    kv *kvs;
    int num;      // kv对数量
    int capacity; //最大容量
} kv_array;
typedef struct
    my_thread_kv_array
{
    kv_array thread_kva[50]; //预留50个线程及对应kv数组
    pid_t thread_id[50];
    int thread_num; //已用的线程数量
} kv_array_of_threads;

/**
 * @brief 主体函数
 *
 */

void mapper(task *t);
void reducer();
void alloc_mappers();
void join_mappers();
char *get_next(char *key, int partition);
char *write_to_local_file(int map_id, int reduce_id, kv_array k);
void read_from_local_file(int reduce_id, int file_num);
int alloc_id(pid_t tid);
void realloc_thread();

/**
 * @brief 工具函数
 */
void sort(kv_array *k);
void quick_sort(kv_array *a, int low, int high);
void kv_arr_init(kv_array *k);
void kv_arr_update(kv_array *k);
void kv_arr_append(kv_array *k, kv *v);

/**
 * @brief 全局变量
 *
 */
Mapper mapperf;
Reducer reducef;
Partitioner hashf;
//三个函数

int reduce_num;
int mapper_num;

kv_array_of_threads tk; //管理线程中的kv对

task *mapper_tasks;                          // map的任务
char intermediate_reduce_files[50][50][100]; //中间文件，预留50*50个
int file_num;                                //输入处理文件数量
pid_t tid;                                   //线程tid

pthread_t p_thread_id[100]; //预留100个线程位

int off[100];
int flag[100];
//合并kv对的标记变量

bool is_free[100]; //判断map线程是否空闲

#endif // __mapreduce_h__
