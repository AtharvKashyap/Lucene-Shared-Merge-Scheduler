package org.apache.lucene.index;

import java.util.concurrent.*;

/**
 * Builds one shared thread-pool for both indexing and merge tasks.
 */
public final class IndexingMergeSharedExecutorService {

  private IndexingMergeSharedExecutorService() {}

  public static ExecutorService newSharedPool(int threads,
                                              int queueCapacity,
                                              boolean prioritizeSmall) {

    BlockingQueue<Runnable> queue;
    if (prioritizeSmall) {
      queue = new PriorityBlockingQueue<>(queueCapacity, (r1, r2) -> {
        boolean m1 = r1 instanceof MergeTaskWrapper;
        boolean m2 = r2 instanceof MergeTaskWrapper;
        if (m1 && m2) {
          return ((MergeTaskWrapper) r1).compareTo((MergeTaskWrapper) r2);
        } else if (m1) {
          return -1;      // merge before generic task
        } else if (m2) {
          return 1;
        }
        return 0;         // both generic → FIFO by insertion
      });
    } else {
      queue = new LinkedBlockingQueue<>(queueCapacity);
    }

    ThreadFactory tf = r -> {
      Thread t = new Thread(r, "lucene-indexing-merge");
      t.setDaemon(true);
      return t;
    };

    return new ThreadPoolExecutor(
        threads, threads,
        30L, TimeUnit.SECONDS,
        queue,
        tf,
        new ThreadPoolExecutor.CallerRunsPolicy());
  }
}
