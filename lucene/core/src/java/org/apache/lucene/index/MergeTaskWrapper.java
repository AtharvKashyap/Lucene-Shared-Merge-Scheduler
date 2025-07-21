package org.apache.lucene.index;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Runnable merge task that tracks its originating {@link IndexWriter}
 * and estimated merge size so it can be prioritised and accounted for.
 */
public final class MergeTaskWrapper
    implements Runnable, Comparable<MergeTaskWrapper> {

  private static final AtomicLong SEQ = new AtomicLong(); // FIFO tiebreaker

  private final SharedMergeScheduler scheduler;
  private final Runnable             delegate;
  private final IndexWriter          writer;
  private final long                 mergeBytes;
  private final long                 seqNo;

  MergeTaskWrapper(SharedMergeScheduler scheduler,
                   Runnable delegate,
                   IndexWriter writer,
                   long mergeBytes) {
    this.scheduler  = scheduler;
    this.delegate   = delegate;
    this.writer     = writer;
    this.mergeBytes = mergeBytes;
    this.seqNo      = SEQ.getAndIncrement();
  }

  /** Executes the merge, then notifies the scheduler. */
  @Override
  public void run() {
    try {
      delegate.run();
    } finally {
      scheduler.onTaskFinished(writer);
    }
  }

  /** Order: smaller merges first, FIFO for equal sizes. */
  @Override
  public int compareTo(MergeTaskWrapper other) {
    int cmp = Long.compare(this.mergeBytes, other.mergeBytes);
    return (cmp != 0) ? cmp
                      : Long.compare(this.seqNo, other.seqNo);
  }

  // --- getters handy for tests / diagnostics -------------------------------
  public IndexWriter getWriter()     { return writer;     }
  public long        getMergeBytes() { return mergeBytes; }
  public long        getSeqNo()      { return seqNo;      }
}
