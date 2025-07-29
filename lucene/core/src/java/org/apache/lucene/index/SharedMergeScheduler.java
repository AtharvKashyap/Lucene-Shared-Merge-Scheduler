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

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * MergeScheduler that funnels merge work from **all** {@link IndexWriter}s into a single shared
 * {@link ExecutorService}.
 *
 * <p>Maps are keyed by {@link MergeScheduler.MergeSource}, which is the object IndexWriter passes
 * back when merges are scheduled. There is one MergeSource instance per writer, so this retains
 * per-writer accounting without relying on casts.
 */
public class SharedMergeScheduler extends MergeScheduler {

  private static final long MIN_BIG_MERGE_BYTES = 50L << 20; // 50 MB

  /** active merge tasks per writer (= per MergeSource) */
  final ConcurrentMap<MergeSource, AtomicInteger> mergeCounts = new ConcurrentHashMap<>();

  /** writers (sources) that have closed but still own in-flight merges */
  final ConcurrentMap<MergeSource, Boolean> closedSources = new ConcurrentHashMap<>();

  private final ExecutorService executor;
  private final boolean shutdownOnClose;

  /**
   * Expert: create a {@code SharedMergeScheduler} that runs merge tasks on the given {@link
   * java.util.concurrent.ExecutorService}.
   *
   * @param executor the thread pool to use for all merges
   * @param shutdownOnClose if {@code true}, {@link #close()} will shut down that pool
   * @lucene.internal
   */
  public SharedMergeScheduler(ExecutorService executor, boolean shutdownOnClose) {
    this.executor = Objects.requireNonNull(executor, "executor");
    this.shutdownOnClose = shutdownOnClose;
  }

  /** Convenience factory that builds a merge-only pool. */
  public static SharedMergeScheduler withDefault(
      int threads, int queueCapacity, boolean prioritizeSmall) {
    ExecutorService es =
        MergeTasksExecutorService.newDefault(threads, queueCapacity, prioritizeSmall);
    return new SharedMergeScheduler(es, /* shutdownOnClose= */ true);
  }

  // ------------------------------------------------------- MergeScheduler ----
  @Override
  public void merge(MergeSource source, MergeTrigger trigger) throws IOException {

    for (; ; ) {
      MergePolicy.OneMerge merge = source.getNextMerge();
      if (merge == null) break;

      mergeCounts.computeIfAbsent(source, k -> new AtomicInteger(k.hashCode())).incrementAndGet();
      Runnable logic =
          () -> {
            try {
              source.merge(merge);

              // opportunistically drain tiny merges
              MergePolicy.OneMerge next;
              while ((next = source.getNextMerge()) != null
                  && next.totalBytesSize() < MIN_BIG_MERGE_BYTES) {
                source.merge(next);
              }
            } catch (IOException ioe) {
              throw new RuntimeException(ioe);
            }
          };

      MergeTaskWrapper task = new MergeTaskWrapper(this, logic, source, merge.totalBytesSize());

      try {
        executor.execute(task);
      } catch (
          @SuppressWarnings("unused")
          RejectedExecutionException ignored) {
        task.run(); // caller-runs fallback
      }
    }
  }

  @Override
  public void close() {
    if (shutdownOnClose) {
      executor.shutdown();
      try {
        if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
          executor.shutdownNow();
        }
      } catch (
          @SuppressWarnings("unused")
          Exception ignored) {
        Thread.currentThread().interrupt();
        executor.shutdownNow();
      }
    }
  }

  // --------------------------------------------------- tracking helpers -----
  /** package-private – called by MergeTaskWrapper when a task completes. */
  void onTaskFinished(MergeSource source) {
    AtomicInteger cnt = mergeCounts.get(source);
    if (cnt != null && cnt.decrementAndGet() == 0 && closedSources.remove(source) != null) {
      mergeCounts.remove(source);
    }
  }

  /** Called from {@code IndexWriter.close()} to mark its MergeSource as finished. */
  public void onWriterClosed(MergeSource source) {
    closedSources.put(source, Boolean.TRUE);
    AtomicInteger cnt = mergeCounts.get(source);
    if (cnt == null || cnt.get() == 0) {
      closedSources.remove(source);
      mergeCounts.remove(source);
    }
  }
}
