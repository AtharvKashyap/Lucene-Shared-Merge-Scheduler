package org.apache.lucene.index;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.index.MergeScheduler.MergeSource;

/**
 * Runnable merge task that knows its originating {@link MergeSource}
 * and estimated merge size so it can be prioritised and accounted for.
 */
public final class MergeTaskWrapper
    implements Runnable, Comparable<MergeTaskWrapper> {

  private static final AtomicLong SEQ = new AtomicLong();   // FIFO tiebreaker

  private final SharedMergeScheduler scheduler;
  private final Runnable             delegate;
  private final MergeSource          source;
  private final long                 mergeBytes;
  private final long                 seqNo;

  MergeTaskWrapper(SharedMergeScheduler scheduler,
                   Runnable delegate,
                   MergeSource source,
                   long mergeBytes) {
    this.scheduler  = scheduler;
    this.delegate   = delegate;
    this.source     = source;
    this.mergeBytes = mergeBytes;
    this.seqNo      = SEQ.getAndIncrement();
  }

  @Override
  public void run() {
    try {
      delegate.run();
    } finally {
      scheduler.onTaskFinished(source);
    }
  }

  @Override
  public int compareTo(MergeTaskWrapper other) {
    int cmp = Long.compare(this.mergeBytes, other.mergeBytes);
    return (cmp != 0) ? cmp
                      : Long.compare(this.seqNo, other.seqNo);
  }

  // test helpers
  public long        getMergeBytes() { return mergeBytes; }
  public long        getSeqNo()      { return seqNo;      }
}
