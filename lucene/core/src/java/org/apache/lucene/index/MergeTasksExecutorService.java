package org.apache.lucene.index;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/** Factory that builds bounded thread pools tuned for merge tasks. */
final class MergeTasksExecutorService {

  private static final AtomicInteger ID = new AtomicInteger();

  private MergeTasksExecutorService() {}

  static ExecutorService newDefault(int threads,
                                    int queueCapacity,
                                    boolean prioritizeSmall) {

    final BlockingQueue<Runnable> queue;
    if (queueCapacity == 0) {
      // No queue at all → caller-runs kicks in immediately
      queue = new SynchronousQueue<>();
    } else if (prioritizeSmall) {
      queue = new PriorityBlockingQueue<>(queueCapacity);
    } else {
      queue = new LinkedBlockingQueue<>(queueCapacity);
    }

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
