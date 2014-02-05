/*
 * CPerQue.h
 *
 *  Created on: Feb 5, 2014
 *      Author: enoyhs
 */

#ifndef CPERQUE_H_
#define CPERQUE_H_

#include <stdio.h> /** printf **/
#include <pthread.h> /** pthread_mutex_init, pthread_mutex_destroy, pthread_mutex_lock, pthread_mutex_unlock **/
#include <sys/stat.h> /** fstat **/
#include <sys/mman.h> /** mmap, munmap **/
#include <fcntl.h> /** open **/
#include <stdlib.h> /** malloc, free **/
#include <string> /** string **/

namespace CPerQue
{
    template <typename Item>
    class CPerQue {
    public:
	struct CPerQue_t {
	    unsigned int size;
	    unsigned int offset;

	    void print() { printf("Size = %d; Offset = %d\n", size, offset); };
	} info;

	CPerQue(std::string _file_name): file_name(_file_name) {
	    pthread_mutex_init(&mtx, NULL);

	    info_fd = open((file_name + "-info").c_str(), O_RDWR | O_CREAT, S_IRWXU | S_IRWXG | S_IXOTH);
	    struct stat sb;
	    if (fstat(info_fd, &sb) == 0 && sb.st_size == 0) {
		// Create info file
		info.size = 0; info.offset = 0;
		write(info_fd, &info, sizeof(info));
	    }
	    info_mmap = (int *) mmap(0, sizeof(info), PROT_READ | PROT_WRITE, MAP_SHARED, info_fd, 0);
	    memcpy(&info, info_mmap, sizeof(info));
	    data_fd = open(file_name.c_str(), O_RDWR | O_APPEND | O_CREAT, S_IRWXU | S_IRWXG | S_IXOTH);
	}
	~CPerQue() {
	    saveInfo();
	    close(data_fd);
	    munmap(info_mmap, sizeof(info));
	    close(info_fd);
	    pthread_mutex_destroy(&mtx);
	}
	void push(Item *value) {
	    pthread_mutex_lock(&mtx);
	    write(data_fd, value, sizeof(Item));
	    info.size++;
	    pthread_mutex_unlock(&mtx);
	    saveInfo();
	}
	void pop() {
	    if (size() > 0) {
		pthread_mutex_lock(&mtx);
		info.offset += sizeof(Item);
		info.size--;
		pthread_mutex_unlock(&mtx);
		saveInfo();
	    }
	}
	Item* front() {
	    if (size() == 0) return NULL;
	    Item *ret = new Item();
	    pthread_mutex_lock(&mtx);
	    lseek(data_fd, info.offset, SEEK_SET);
	    read(data_fd, ret, sizeof(Item));
	    pthread_mutex_unlock(&mtx);
	    return ret;
	}
	Item* get(unsigned int idx) {
	    if (size() == 0 || idx < 0 || idx >= size()) return NULL;
	    Item *ret = new Item();
	    pthread_mutex_lock(&mtx);
	    lseek(data_fd, info.offset + idx * sizeof(Item), SEEK_SET);
	    read(data_fd, ret, sizeof(Item));
	    pthread_mutex_unlock(&mtx);
	    return ret;
	}
	int size() {
	    return info.size;
	}
	bool empty() {
	    return size() == 0;
	}
	// Saves info to file, calls locks, so they should be freed when calling this method
	void clear() {
	    pthread_mutex_lock(&mtx);
	    ftruncate(data_fd, 0);
	    info.size = 0;
	    info.offset = 0;
	    idx_offset.idx = 0; idx_offset.offset = 0; // Reset idx_offset cache
	    pthread_mutex_unlock(&mtx);
	}
    private:
	// Saves info to file, calls locks, so they should be freed when calling this method
	void saveInfo() {
	    // If queue is empty just truncate the data file, so that we do not waste space
	    if (info.size == 0) clear();
	    pthread_mutex_lock(&mtx);
	    memcpy(info_mmap, &info, sizeof(info));
	    msync(info_mmap, sizeof(info), MS_ASYNC);
	    pthread_mutex_unlock(&mtx);
	}

	pthread_mutex_t mtx;
	std::string file_name;
	int data_fd;
	int info_fd;
	void *info_mmap;

	// Optimise continuous access with get(int idx) method
	// This is so we don't have to rexamine queue, when queue is of varying size elements
	// pair of idx and its offset
	struct idx_offset_t {
	    unsigned int idx;
	    long long int offset;
	} idx_offset;
    };

} /* namespace CPerQue */

#endif /* CPERQUE_H_ */
