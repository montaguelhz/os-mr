#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <pthread.h>
#include <stdbool.h>
#include <assert.h>
#include <errno.h>
#include "mapreduce.h"
#define new 0
#define running 1
#define finished 2
#define die 3

/**
 * @brief 实现数组和指针间的值的复制
 *
 * @param arr 被赋值
 * @param ptr 赋值
 */
#define ptr_and_arr_type_cast(arr, ptr)                          \
    do                                                           \
    {                                                            \
        int defined_i;                                           \
        for (defined_i = 0; ptr[defined_i] != '\0'; defined_i++) \
        {                                                        \
            arr[defined_i] = ptr[defined_i];                     \
        }                                                        \
        arr[defined_i] = '\0';                                   \
    } while (false)

pthread_mutex_t thread_mutex = PTHREAD_MUTEX_INITIALIZER; // tk线程锁
void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce, int num_reducers, Partitioner partition)
{

    mapperf = map;
    reducef = reduce;
    hashf = partition;
    reduce_num = num_reducers;
    file_num = argc - 1;
    if (num_mappers > file_num)
    {
        num_mappers = file_num;
    }
    mapper_num = num_mappers;
    //全局变量初始化

    mapper_tasks = (task *)malloc(sizeof(task) * file_num);
    tk.thread_num = 0;
    for (int i = 1; i < argc; i++)
    {
        mapper_tasks[i - 1].state = new;
        mapper_tasks[i - 1].input = argv[i];
        mapper_tasks[i - 1].task_id = i - 1;
        mapper_tasks[i - 1].pthread_id = -1;
    }
    for (int i = 0; i < mapper_num; i++)
    {
        is_free[i] = true;
    }
    //任务准备

    pthread_create(&p_thread_id[argc], NULL, (void *)alloc_mappers, NULL);
    pthread_create(&p_thread_id[argc + 1], NULL, (void *)join_mappers, NULL);
    pthread_join(p_thread_id[argc], NULL);
    pthread_join(p_thread_id[argc + 1], NULL);
    //开启两个线程分配线程处理map任务
    realloc_thread();
    free(mapper_tasks);
    mapper_tasks = NULL;
    //回收指针

    memset(flag, 0, sizeof(int) * 100);
    memset(off, 0, sizeof(int) * 100);
    //标记变量初始化

    for (int i = 0; i < num_reducers; i++)
    {
        pthread_create(&p_thread_id[i], NULL, (void *)reducer, NULL);
    }
    for (int i = 0; i < num_reducers; i++)
    {
        pthread_join(p_thread_id[i], NULL);
    }
}

/**
 * @brief mapper函数 分割出与reduce相同数量的中间文件
 *
 * @param the_mapper_task
 */
void mapper(task *the_mapper_task)
{

    int id = alloc_id(syscall(SYS_gettid));
    mapperf(the_mapper_task->input, id);
    kv_array k = tk.thread_kva[id];
    kv_array *buf = (kv_array *)malloc(sizeof(kv_array) * reduce_num);
    //
    for (int i = 0; i < reduce_num; i++)
    {
        kv_arr_init(&buf[i]);
    }
    int num = 0;
    for (int i = 0; i < k.num; i++)
    {
        num = hashf(k.kvs[i].key, reduce_num);
        kv_arr_append(&buf[num], &k.kvs[i]);
    }
    the_mapper_task->mediate = (char **)malloc(sizeof(char *) * reduce_num);
    for (int i = 0; i < reduce_num; i++)
    {
        the_mapper_task->mediate[i] = write_to_local_file(the_mapper_task->task_id,
                                                          i, buf[i]);
    }

    the_mapper_task->state = finished;
    pthread_exit("");
    //标记结束
}

/**
 * @brief reduce函数 读取对应中间文件合并计数kv对
 *
 */
void reducer()
{
    int id = alloc_id(syscall(SYS_gettid));
    read_from_local_file(id, file_num);
    kv_array k = tk.thread_kva[id];
    sort(&k);
    while (flag[id] < k.num)
    {
        reducef(k.kvs[flag[id]].key, get_next, id);
    }
    pthread_exit("");
}

/**
 * @brief 轮询分配mapper线程
 *
 */
void alloc_mappers()
{
    int j = 0;
    for (int i = 0; i < file_num; i++)
    {
        while (true)
        {

            if (is_free[j])
            {
                is_free[j] = false;
                mapper_tasks[i].state = running;
                mapper_tasks[i].pthread_id = j;
                pthread_create(&p_thread_id[j], NULL,
                               (void *)mapper, (void *)&mapper_tasks[i]);
                break;
            }
            j++;
            j %= mapper_num;
        }
    }
    pthread_exit("");
}

/**
 * @brief 轮询回收线程
 *
 */
void join_mappers()
{

    int unfinished = file_num;
    int i = 0;
    while (unfinished)
    {
        if (mapper_tasks[i].state == finished)
        {

            pthread_join(p_thread_id[mapper_tasks[i].pthread_id], NULL);
            mapper_tasks[i].state = die;
            unfinished--;
            is_free[mapper_tasks[i].pthread_id] = true;

            for (int j = 0; j < reduce_num; j++)
            {

                ptr_and_arr_type_cast(intermediate_reduce_files[mapper_tasks[i].task_id][j],
                                      mapper_tasks[i].mediate[j]);
            }
        }
        i++;
        i %= file_num;
    }
    pthread_exit("");
}

/**
 * @brief 对kv对数组往下读
 *
 * @param key
 * @param id
 * @return char* 读到一组相同key的kv对结束返回空，否则返回1
 */
char *get_next(char *key, int id)
{
    kv_array *k = &tk.thread_kva[id];
    if (off[id])
    {
        flag[id]++;
        off[id] = 0;
        return NULL;
    }
    else
    {
        if (flag[id] < k->num && !strcmp(k->kvs[flag[id]].key, key))
            ;
        else
        {
            off[id] = 1;
        }
        flag[id]++;
        return "1";
    }
}
/**
 * @brief 将key存入kv对数组
 *
 * @param key
 * @param value
 * @param id
 */
void MR_Emit(char *key, char *value, int id)
{
    if (key[0] == '\0')
        return;
    int num = tk.thread_kva[id].num;

    ptr_and_arr_type_cast(tk.thread_kva[id].kvs[num].key, key);
    ptr_and_arr_type_cast(tk.thread_kva[id].kvs[num].value, value);
    __sync_fetch_and_add(&tk.thread_kva[id].num, 1);
    kv_arr_update(&tk.thread_kva[id]);
}

/**
 * @brief 初始化tk的kv对数组
 *
 * @param tid
 * @return int
 */
int alloc_id(int tid)
{
    pthread_mutex_lock(&thread_mutex);
    for (int i = 0; i < tk.thread_num; i++)
    {
        if (tid == tk.thread_id[i])
        {
            pthread_mutex_unlock(&thread_mutex);
            return i;
        }
    }
    __sync_fetch_and_add(&tk.thread_num, 1);
    tk.thread_id[tk.thread_num - 1] = tid;

    kv_arr_init(&tk.thread_kva[tk.thread_num - 1]);
    pthread_mutex_unlock(&thread_mutex);
    return tk.thread_num - 1;
}

/**
 * @brief 回收指针资源
 *
 */
void realloc_thread()
{
    for (int i = 0; i < tk.thread_num; i++)
    {
        free(tk.thread_kva[i].kvs);
        tk.thread_kva[i].kvs = NULL;
        tk.thread_kva[i].num = 0;
        tk.thread_kva[i].capacity = 0;
    }
    tk.thread_num = 0;
}
/**
 * @brief 中间文件写入test-out文件夹，以mr-map_task_id-reduce_id的名字保存
 *
 * @param task_id
 * @param reduce_id
 * @param k
 * @return char*
 */
char *write_to_local_file(int task_id, int reduce_id, kv_array k)
{

    int fd;
    char filename[200];
    char path[100];
    char *name = NULL;
    name = (char *)malloc(sizeof(char) * 200);
    getcwd(path, 100);
    sprintf(filename, "%s/test-out/mr-%d-%d", path, task_id, reduce_id);
    FILE *fp = NULL;
    fp = fopen(filename, "w+");
    assert(fp != NULL);
    for (int i = 0; i < k.num; i++)
    {
        fputs(k.kvs[i].key, fp);
        fputc(' ', fp);
        fputs(k.kvs[i].value, fp);
        fputc('\n', fp);
    }
    fclose(fp);
    ptr_and_arr_type_cast(name, filename);
    return name;
}
/**
 * @brief 将多个对应文件全部读取至线程对应数组中
 *
 * @param reduce_id
 * @param files_num
 */
void read_from_local_file(int reduce_id, int files_num)
{

    kv_array *k = &tk.thread_kva[reduce_id];
    FILE *fp;
    char buf[100];
    for (int task_id = 0; task_id < files_num; task_id++)
    {
        char *f = intermediate_reduce_files[task_id][reduce_id];
        fp = fopen(f, "r");
        while (fgets(buf, 100, fp))
        {
            kv tmp;
            int j;
            for (j = 0; buf[j] != ' '; j++)
            {
                tmp.key[j] = buf[j];
            }
            tmp.key[j] = '\0';
            j++;
            int t = j;
            for (; buf[j] != '\n'; j++)
            {
                tmp.value[j - t] = buf[j];
            }
            tmp.value[j - t] = '\0';

            kv_arr_append(k, &tmp);
        }
        fclose(fp);
    }
}

/**
 * @brief
 *
 * @param k
 */
inline void kv_arr_init(kv_array *k)
{
    k->num = 0;
    k->capacity = 2000;
    k->kvs = (kv *)malloc(k->capacity * sizeof(kv));
}
/**
 * @brief
 *
 * @param k
 */
inline void kv_arr_update(kv_array *k)
{
    if (k->num >= k->capacity - 500)
    {
        k->capacity *= 2;

        kv *tmp = (kv *)realloc(k->kvs, k->capacity * sizeof(kv));
        assert(tmp != NULL);

        k->kvs = tmp;
    }
}
/**
 * @brief
 *
 * @param k
 * @param v
 */
inline void kv_arr_append(kv_array *k, kv *v)
{
    ptr_and_arr_type_cast(k->kvs[k->num].key, v->key);
    ptr_and_arr_type_cast(k->kvs[k->num].value, v->value);
    __sync_fetch_and_add(&k->num, 1);
    kv_arr_update(k);
}
/**
 * @brief 暂时用快排
 *
 * @param k
 */
void sort(kv_array *k)
{
    quick_sort(k, 0, k->num - 1);
}
/**
 * @brief 快排
 *
 * @param a
 * @param low
 * @param high
 */
void quick_sort(kv_array *a, int low, int high)
{

    if (low < high)
    {
        int i = low;
        int j = high;
        kv k = a->kvs[low];
        while (i < j)
        {

            while (i < j && strcmp(a->kvs[j].key, k.key) >= 0) // 从右向左找第一个小于k的数
            {
                j--;
            }

            if (i < j)
            {

                a->kvs[i++] = a->kvs[j];
            }
            while (i < j && strcmp(a->kvs[i].key, k.key) < 0) // 从左向右找第一个大于等于k的数
            {
                i++;
            }

            if (i < j)
            {
                a->kvs[j] = a->kvs[i];

                j--;
            }
        }

        a->kvs[i] = k;

        // 递归调用
        quick_sort(a, low, i - 1);  // 排序k左边
        quick_sort(a, i + 1, high); // 排序k右边
    }
}
