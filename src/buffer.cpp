/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University
 * of Wisconsin-Madison.
 */

#include "buffer.h"

#include <iostream>
#include <memory>

#include "exceptions/bad_buffer_exception.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/hash_not_found_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"

namespace badgerdb {

constexpr int HASHTABLE_SZ(int bufs) { return ((int)(bufs * 1.2) & -2) + 1; }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(std::uint32_t bufs)
    : numBufs(bufs),
      hashTable(HASHTABLE_SZ(bufs)),
      bufDescTable(bufs),
      bufPool(bufs) {
  for (FrameId i = 0; i < bufs; i++) {
    bufDescTable[i].frameNo = i;
    bufDescTable[i].valid = false;
  }

  clockHand = bufs - 1;
}

/**
 * This method advances the clockHand to the next bit using modular
 * arithmetic to ensure the clock remains in the acceptable range
 */
void BufMgr::advanceClock() {
    this->clockHand = (this->clockHand + 1) % this->numBufs
}

void BufMgr::allocBuf(FrameId& frame) {

    // find free frame using clock algorithm

    // write page back to disk, dirty if necessary

    // if buffer frame has valid page in it, remove entry from hash table

    // Throw BufferExceededException if all buffer frames are pinned
}

/**
 * Reads the given page from the file into a frame and returns the pointer to the page.
 * If the requested page is already present in the buffer pool, the pointer to that frame is returned.
 * Otherwise, a new frame is allocated from the buffer pool for reading the page .
 * @param file to be allocated in the buffer manager
 * @param pageNo is the page number that will be allocated
 * @param page that will be allocated
 *
 * Returns the pointer to the page being retrieved
 */
void BufMgr::readPage(File& file, const PageId pageNo, Page*& page) {
    
    int frameId = bufDescTable[clockHand].frameNo; // fetch the current frame Id
    bool isThrown = false; // default value is false, made true if exception is thrown
    
    // wrapping the page-fetching process inside a try-catch block to deal with the case when the page is not found
    try {
        // fetch pointer to page we're trying to read
        hashTable->lookup(file, pageNo, frameId);
    } catch (HashNotFoundException err) {
        // new frame is allocated from the buffer pool for reading page not present
        allocBuf(frameId);
        Page pageTemp = file.readPage(pageNo);
        bufPool[frameId] = pageTemp;
        this->hashTable.insert(file, pageNo, frameId);
        bufDescTable[frameId].Set(file, pageNo);
        isThrown = true; // change isThrown to true
    }
    
    if(!isThrown) {
        // increase pin count by 1 and change refbit to true if this requested page is already present in the buffer pool
        bufDescTable[frameId].pinCnt++;
        bufDescTable[frameId].refbit = true;
    }
    
    // return pointer to the page being retrieved
    page = &bufPool[frameId];
}

void BufMgr::unPinPage(File& file, const PageId pageNo, const bool dirty) {}

/**
 * Allocate a page within the buffer manager
 * @param file to be allocated in the buffer manager
 * @param pageNo is the page number that will be allocated
 * @param page that will be allocated
 *
 * Returns pageNo and page by updating the pointers
 */
void BufMgr::allocPage(File& file, PageId& pageNo, Page*& page) {
    Page*& newPage = file.allocatePage();
    int frameNo = bufDescTable[clockHand].frameNo;
    allocBuf(frameNo) // I think this is how I'm supposed to do this...

    // Wrap in try catch
    this->hashTable.insert(file, pageNo, frameNo);
    this->hashTable.Set(file, pageNo)

    // set pageNo and page?
}

/**
 * Scans buffer and removes pages belonging to the given file from hashtable and BufDesc.
 * If page is dirty, write the page back.
 *
 * @param file file to be flushed
 * @throws PagePinnedException if some page of the file is pinned.
 * @throws BadBufferException if an invalid page belonging to the file is encountered.
 */
void BufMgr::flushFile(File& file) {
    for (FrameId index = 0; index < numBufs; index++) {
        // first check that page belongs to the given file
        if (file == bufDescTable[index].file){
            // throw exception if page is invalid or pinned
            if (!bufDescTable[index].valid) {
                throw BadBufferException(index, bufDescTable[index].dirty, bufDescTable[index].valid, bufDescTable[index].refbit);
            }
            else if (bufDescTable[index].pinCnt >= 1) {
                throw PagePinnedException(file->filename(), bufDescTable[index].pageNo, index)
            }
            if (bufDescTable[index].dirty){
                // if dirty, write page back and set dirty bit to false
                bufDescTable[index].file -> writePage(bufPool[index]);
                bufDescTable[index].dirty = false;
                // then remove page from hashtable and bufDescTable
                hashtable -> remove(file, bufDescTable[index].pageNo);
                bufDescTable[index].Clear();
            }
        }
    }
}

/**
 * Deletes a particular page from file.
 * If page is allocated a frame in the buffer pool, also frees the frame and removes entry from hashtable
 *
 * @param file file that the page is found in
 * @param PageNo page number of the page to be deleted
 */
void BufMgr::disposePage(File& file, const PageId PageNo) {
    FrameId frameId;
    try{
        hashTable -> lookup(file, PageNo, frameNo);
        // page is in the buffer pool, now free and remove
        bufDescTable[frameId].Clear();
        hashtable -> remove(file, PageNo);
        // lastly delete page from file
        file -> deletePage(PageNo);
    } catch(HashNotFoundException e){ //  page to be deleted is not allocated a frame in the buffer pool
        // no need to throw exception if the hash is not found
    }

}

void BufMgr::printSelf(void) {
  int validFrames = 0;

  for (FrameId i = 0; i < numBufs; i++) {
    std::cout << "FrameNo:" << i << " ";
    bufDescTable[i].Print();

    if (bufDescTable[i].valid) validFrames++;
  }

  std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}  // namespace badgerdb
