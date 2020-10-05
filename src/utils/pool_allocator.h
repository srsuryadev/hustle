// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef HUSTLE_POOL_ALLOCATOR_H
#define HUSTLE_POOL_ALLOCATOR_H

#include <stdlib.h>

#include <cstddef>
#include <mutex>
/**
 * A chunk within a larger block.
 */
struct Chunk {
  /**
   * When a chunk is free, the `next` contains the
   * address of the next chunk in a list.
   *
   * When it's allocated, this space is used by
   * the user.
   */
  Chunk *next;
};

static const uint32_t kNumSlots = 520;
static const uint32_t kSlotSize = 520 * 1024;
/**
 * The allocator class.
 *
 * Features:
 *
 *   - Parametrized by number of chunks per block
 *   - Keeps track of the allocation pointer
 *   - Bump-allocates chunks
 *   - Requests a new larger block when needed
 *
 */
class PoolAllocator {
 public:
  static PoolAllocator &getInstance() {
    static PoolAllocator instance{0};
    return instance;
  }

  void *allocate(int index, int chunk_index) {
    if (index == 0) {
      return pool0[chunk_index];
    }
    return pool1[chunk_index];
  }
  void *custom_allocate(size_t size) {
    std::lock_guard<std::mutex> guard(mutex_);
    // No chunks left in the current block, or no any block
    // exists yet. Allocate a new one, passing the chunk size:

    if (mAlloc == nullptr) {
      mAlloc = allocateBlock(size);
    }

    // The return value is the current position of
    // the allocation pointer:

    Chunk *freeChunk = mAlloc;

    // Advance (bump) the allocation pointer to the next chunk.
    //
    // When no chunks left, the `mAlloc` will be set to `nullptr`, and
    // this will cause allocation of a new block on the next request:

    mAlloc = mAlloc->next;

    return freeChunk;
  }

  void deallocate(void *chunk, size_t size) {
    std::lock_guard<std::mutex> guard(mutex_);
    // The freed chunk's next pointer points to the
    // current allocation pointer:

    reinterpret_cast<Chunk *>(chunk)->next = mAlloc;

    // And the allocation pointer is moved backwards, and
    // is set to the returned (now free) chunk:

    mAlloc = reinterpret_cast<Chunk *>(chunk);
  }

 private:
  PoolAllocator(size_t chunksPerBlock) : mChunksPerBlock(chunksPerBlock) {
    initialize();
  }
  void initialize() {
    // allocate(512 * 1024);

    for (size_t i = 0; i < kNumSlots; i++) {
      pool0[i] = (char *)malloc(kSlotSize);
      pool1[i] = (char *)malloc(kSlotSize);
    }
  }

  char *pool0[kNumSlots];
  char *pool1[kNumSlots];
  /**
   * Number of chunks per larger block.
   */
  size_t mChunksPerBlock;
  std::mutex mutex_;

  /**
   * Allocation pointer.
   */
  Chunk *mAlloc = nullptr;

  /**
   * Allocates a larger block (pool) for chunks.
   */
  Chunk *allocateBlock(size_t chunkSize) {
    // cout << "\nAllocating block (" << mChunksPerBlock << " chunks):\n\n";

    size_t blockSize = mChunksPerBlock * chunkSize;

    // The first chunk of the new block.
    Chunk *blockBegin = reinterpret_cast<Chunk *>(malloc(blockSize));

    // Once the block is allocated, we need to chain all
    // the chunks in this block:

    Chunk *chunk = blockBegin;

    for (int i = 0; i < mChunksPerBlock - 1; ++i) {
      chunk->next = reinterpret_cast<Chunk *>(reinterpret_cast<char *>(chunk) +
                                              chunkSize);
      chunk = chunk->next;
    }

    chunk->next = nullptr;

    return blockBegin;
  }
};

#endif  // HUSTLE_POOL_ALLOCATOR_H