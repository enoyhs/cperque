/*
 * CPerQue.cpp
 *
 *  Created on: Feb 5, 2014
 *      Author: enoyhs
 */

#include "CPerQue.h"

namespace CPerQue
{
    template<> void CPerQue<std::string>::push(std::string *value) {
	pthread_mutex_lock(&mtx);
	unsigned int size = value->size();
	write(data_fd, &size, sizeof(size));
	write(data_fd, value->c_str(), sizeof(char) * size);
	info.size++;
	pthread_mutex_unlock(&mtx);
	saveInfo();
    }
    template<> void CPerQue<std::string>::pop() {
	if (size() > 0) {
	    pthread_mutex_lock(&mtx);
	    unsigned int size = 0;
	    lseek(data_fd, info.offset, SEEK_SET);
	    read(data_fd, &size, sizeof(size));
	    info.offset += sizeof(size) + sizeof(char) * size;
	    info.size--;
	    // Update idx_offset cache. When we remove element from front, idx decrease by 1, but the offset stays the same.
	    idx_offset.idx--;
	    pthread_mutex_unlock(&mtx);
	    saveInfo();
	}
    }
    template<> std::string* CPerQue<std::string>::front() {
	if (size() == 0) return NULL;
	unsigned int size = 0;
	pthread_mutex_lock(&mtx);
	lseek(data_fd, info.offset, SEEK_SET);
	read(data_fd, &size, sizeof(size));
	char *val = (char*)malloc(sizeof(char) * (size + 1));
	val[size] = 0;
	read(data_fd, val, sizeof(char) * size);
	pthread_mutex_unlock(&mtx);
	std::string *ret = new std::string(val, size);
	free(val);
	return ret;
    }
    template<> std::string* CPerQue<std::string>::get(unsigned int idx) {
	if (size() == 0 || idx < 0 || idx >= size()) return NULL;
	pthread_mutex_lock(&mtx);
	unsigned int size = 0;
	unsigned int offset = info.offset;
	// Check offset cache (it should be accurate if we are accesing elements consecutively
	if (idx_offset.idx == idx) {
	    // Set offset cache to next idx (as we will increase offset to the next possible idx)
	    idx_offset.idx = idx + 1;
	    idx = 0; // Set idx to as we only need 1 iteration to get the size of string
	    offset = idx_offset.offset; // Update offset to correct position
	} else {
	    // Set offset cache to next idx (as we will increase offset to the next possible idx)
	    idx_offset.idx = idx + 1;
	}
	while (idx >= 0) {
	    lseek(data_fd, offset, SEEK_SET);
	    read(data_fd, &size, sizeof(size));
	    offset += sizeof(size) + sizeof(char) * size;
	    idx--;
	}
	// Save this offset to cache
	idx_offset.offset = offset;
	char *val = (char*)malloc(sizeof(char) * (size + 1));
	val[size] = 0;
	read(data_fd, val, sizeof(char) * size);
	pthread_mutex_unlock(&mtx);
	std::string *ret = new std::string(val, size);
	free(val);
	return ret;
    }

} /* namespace CPerQue */
