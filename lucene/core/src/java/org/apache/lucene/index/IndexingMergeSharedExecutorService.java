/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lucene.index;

import java.util.concurrent.*;

/** Builds one shared thread-pool for both indexing and merge tasks. */
public final class IndexingMergeSharedExecutorService {

  private IndexingMergeSharedExecutorService() {}

  /**
   * Builds a single thread-pool to handle both indexing and merge tasks.
   *
   * <p>If {@code prioritizeSmall} is true, merge tasks are ordered by size, otherwise FIFO.
   *
   * @param threads maximum concurrent threads
   * @param queueCapacity max queued tasks before caller-runs fallback
   * @param prioritizeSmall whether to prioritize smaller merges
   * @lucene.experimental
   */
  public static ExecutorService newSharedPool(
      int threads, int queueCapacity, boolean prioritizeSmall) {

    BlockingQueue<Runnable> queue;
    if (prioritizeSmall) {
      queue =
          new PriorityBlockingQueue<>(
              queueCapacity,
              (r1, r2) -> {
                boolean m1 = r1 instanceof MergeTaskWrapper;
                boolean m2 = r2 instanceof MergeTaskWrapper;
                if (m1 && m2) {
                  return ((MergeTaskWrapper) r1).compareTo((MergeTaskWrapper) r2);
                } else if (m1) {
                  return -1; // merge before generic task
                } else if (m2) {
                  return 1;
                }
                return 0; // both generic → FIFO by insertion
              });
    } else {
      queue = new LinkedBlockingQueue<>(queueCapacity);
    }

    ThreadFactory tf =
        r -> {
          Thread t = new Thread(r, "lucene-indexing-merge");
          t.setDaemon(true);
          return t;
        };

    return new ThreadPoolExecutor(
        threads,
        threads,
        30L,
        TimeUnit.SECONDS,
        queue,
        tf,
        new ThreadPoolExecutor.CallerRunsPolicy());
  }
}
