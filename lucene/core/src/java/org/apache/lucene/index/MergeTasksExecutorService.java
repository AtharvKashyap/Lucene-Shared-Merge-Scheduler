package java.org.apache.lucene.index;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Factory that creates bounded thread-pools optimised for merge tasks.
 */
final class MergeTasksExecutorService {

  private static final AtomicInteger ID = new AtomicInteger();

  private MergeTasksExecutorService() {}   // no instances

  /**
   * @param threads          maximum merge threads
   * @param queueCapacity    max queued tasks before CallerRuns kicks in
   * @param prioritizeSmall  true ⇒ use a Priority queue (small merges first)
   */
  static ExecutorService newDefault(int threads,
                                    int queueCapacity,
                                    boolean prioritizeSmall) {

    BlockingQueue<Runnable> queue = prioritizeSmall
        ? new PriorityBlockingQueue<>(queueCapacity)
        : new LinkedBlockingQueue<>(queueCapacity);

    ThreadFactory tf = r -> {
      Thread t = new Thread(r, "lucene-shared-merge-" + ID.incrementAndGet());
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
