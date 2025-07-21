package java.org.apache.lucene.index;

import java.util.concurrent.*;

/**
 * Builds a single thread-pool for **both** indexing and merge work.
 *
 * <p>You simply submit normal indexing Runnables *and* {@link MergeTaskWrapper}s
 * to this pool; it enforces one hard thread limit for all activity.</p>
 *
 * <p>Merge tasks are given **higher priority** when the queue is backed by a
 * {@link PriorityBlockingQueue} (default).  If you prefer strict FIFO, pass
 * {@code prioritizeSmall=false}.</p>
 */
public final class IndexingMergeSharedExecutorService {

  private IndexingMergeSharedExecutorService() {}  // no instances

  /**
   * @param threads           total CPU threads for indexing + merging
   * @param queueCapacity     max queued tasks
   * @param prioritizeSmall   true ⇒ small merges jump ahead of large merges
   */
  public static ExecutorService newSharedPool(int threads,
                                              int queueCapacity,
                                              boolean prioritizeSmall) {

    BlockingQueue<Runnable> queue =
        new PriorityBlockingQueue<>(queueCapacity, (r1, r2) -> {
          boolean m1 = r1 instanceof MergeTaskWrapper;
          boolean m2 = r2 instanceof MergeTaskWrapper;
          if (m1 && m2) {
            return ((MergeTaskWrapper) r1).compareTo((MergeTaskWrapper) r2);
          } else if (m1) {          // favour merge over generic indexing task
            return -1;
          } else if (m2) {
            return 1;
          }
          return 0;                 // both generic → FIFO via queue insertion
        });

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
