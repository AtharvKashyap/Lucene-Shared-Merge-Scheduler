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
import java.util.concurrent.atomic.AtomicInteger;

/** Factory that builds bounded thread pools tuned for merge tasks. */
final class MergeTasksExecutorService {

  private static final AtomicInteger ID = new AtomicInteger();

  private MergeTasksExecutorService() {}

  static ExecutorService newDefault(int threads, int queueCapacity, boolean prioritizeSmall) {

    final BlockingQueue<Runnable> queue;
    if (queueCapacity == 0) {
      // No queue at all → caller-runs kicks in immediately
      queue = new SynchronousQueue<>();
    } else if (prioritizeSmall) {
      queue = new PriorityBlockingQueue<>(queueCapacity);
    } else {
      queue = new LinkedBlockingQueue<>(queueCapacity);
    }

    ThreadFactory tf =
        r -> {
          Thread t = new Thread(r, "lucene-shared-merge-" + ID.incrementAndGet());
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
