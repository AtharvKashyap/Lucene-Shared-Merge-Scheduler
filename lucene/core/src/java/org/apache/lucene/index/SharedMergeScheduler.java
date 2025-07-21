package org.apache.lucene.index;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * MergeScheduler that funnels all merges from every {@link IndexWriter}
 * into a single shared {@link ExecutorService}.
 *
 * <p>Choose one of:<br>
 * • {@link #withDefault(int,int,boolean)} – merge-only pool<br>
 * • {@link IndexingMergeSharedExecutorService} – mixed indexing+merge pool<br>
 * – or supply any other ExecutorService you control.</p>
 *
 * <p>Back-pressure: when the queue is full the submitting indexing thread
 * executes the merge inline, naturally throttling that writer.</p>
 */
public class SharedMergeScheduler extends MergeScheduler {

  private static final long MIN_BIG_MERGE_BYTES = 50L << 20; // 50 MB

  /** active merges per writer */
  private final ConcurrentMap<IndexWriter, AtomicInteger> mergeCounts =
      new ConcurrentHashMap<>();

  /** writers already closed, pending last merge completion */
  private final ConcurrentMap<IndexWriter,Boolean> closedWriters =
      new ConcurrentHashMap<>();

  private final ExecutorService executor;
  private final boolean shutdownOnClose;

  // ------------------------------------------------------------------ ctor --

  public SharedMergeScheduler(ExecutorService executor,
                              boolean shutdownOnClose) {
    this.executor         = Objects.requireNonNull(executor, "executor");
    this.shutdownOnClose  = shutdownOnClose;
  }

  /** Convenience factory: merge-only pool. */
  public static SharedMergeScheduler withDefault(int threads,
                                                 int queueCapacity,
                                                 boolean prioritizeSmall) {
    ExecutorService es = MergeTasksExecutorService
        .newDefault(threads, queueCapacity, prioritizeSmall);
    return new SharedMergeScheduler(es, /*shutdownOnClose=*/true);
  }

  // ----------------------------------------------------------- MergeScheduler

  @Override
  public void merge(MergeSource source, MergeTrigger trigger) throws IOException {
    IndexWriter writer = (IndexWriter) source; // IndexWriter implements MergeSource

    for (;;) {
      MergePolicy.OneMerge merge = source.getNextMerge();
      if (merge == null) break;

      // track active count
      mergeCounts.computeIfAbsent(writer, w -> new AtomicInteger())
                 .incrementAndGet();

      Runnable logic = () -> {
        try {
          source.merge(merge);

          // opportunistic drain of tiny merges (CMS behaviour)
          MergePolicy.OneMerge next;
          while ((next = source.getNextMerge()) != null
                 && next.totalBytesSize() < MIN_BIG_MERGE_BYTES) {
            source.merge(next);
          }
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
      };

      MergeTaskWrapper task =
          new MergeTaskWrapper(this, logic, writer, merge.totalBytesSize());

      try {
        executor.execute(task);
      } catch (RejectedExecutionException e) {
        // queue full -> run inline (caller-runs)
        task.run();
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
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        executor.shutdownNow();
      }
    }
  }

  // ----------------------------------------------------------- writer hooks -

  /** Called by {@link MergeTaskWrapper} after each task completes. */
  void onTaskFinished(IndexWriter writer) {
    AtomicInteger cnt = mergeCounts.get(writer);
    if (cnt != null && cnt.decrementAndGet() == 0 && closedWriters.remove(writer) != null) {
      mergeCounts.remove(writer);
    }
  }

  /** Call from {@code IndexWriter.close()} to mark writer as finished. */
  public void onWriterClosed(IndexWriter writer) {
    closedWriters.put(writer, Boolean.TRUE);
    AtomicInteger cnt = mergeCounts.get(writer);
    if (cnt == null || cnt.get() == 0) {
      mergeCounts.remove(writer);
      closedWriters.remove(writer);
    }
  }
}
