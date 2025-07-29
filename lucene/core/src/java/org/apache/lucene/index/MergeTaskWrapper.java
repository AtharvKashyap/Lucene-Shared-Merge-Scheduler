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

import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.index.MergeScheduler.MergeSource;

/**
 * Runnable merge task that knows its originating {@link MergeSource} and estimated merge size so it
 * can be prioritised and accounted for.
 */
public final class MergeTaskWrapper implements Runnable, Comparable<MergeTaskWrapper> {

  private static final AtomicLong SEQ = new AtomicLong(); // FIFO tiebreaker

  private final SharedMergeScheduler scheduler;
  private final Runnable delegate;
  private final MergeSource source;
  private final long mergeBytes;
  private final long seqNo;

  MergeTaskWrapper(
      SharedMergeScheduler scheduler, Runnable delegate, MergeSource source, long mergeBytes) {
    this.scheduler = scheduler;
    this.delegate = delegate;
    this.source = source;
    this.mergeBytes = mergeBytes;
    this.seqNo = SEQ.getAndIncrement();
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
    return (cmp != 0) ? cmp : Long.compare(this.seqNo, other.seqNo);
  }

  /** Returns the estimated size in bytes of this merge. @lucene.internal */
  public long getMergeBytes() {
    return mergeBytes;
  }

  /** Returns this task’s FIFO sequence number. @lucene.internal */
  public long getSeqNo() {
    return seqNo;
  }
}
