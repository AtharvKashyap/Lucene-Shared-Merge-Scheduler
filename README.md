<!--
    Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    the "License"); you may not use this file except in compliance with
    the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
-->

# Lucene – Shared Merge Scheduler

> This repo showcases my work on a multi-tenant merge scheduler for Apache Lucene.  
> Upstream branch: `cms-manager-atharv` at https://github.com/yaser-aj/lucene  
> License: Apache 2.0

## Why this exists
On Solr/Elasticsearch/OpenSearch clusters you often run many `IndexWriter`s in the same JVM. Lucene’s default `ConcurrentMergeScheduler` sizes threads like there’s only one writer, which leads to:
- Too many merge threads in total
- Unfairness across indexes
- CPU contention that’s hard to rein in vs. indexing threads

**What we built:** a shared, globally capped merge scheduler. All merges go through one bounded executor so you can control total merge work, reduce contention, and keep throughput steady.

## How it works (high level)
- **SharedMergeScheduler**  
  Routes merges from all writers into one `ExecutorService`. Tracks active merges per writer using `MergeScheduler.MergeSource`.  
  Cleans up when a writer closes. If the queue is full, merges run inline (Caller-Runs) to push back on the hot writer.  
  Executes very small merges (~<50 MB) immediately to avoid queue churn.  
  You can pass in your own executor, or use `withDefault(threads, queueCapacity, prioritizeSmall)`.

- **MergeTaskWrapper**  
  Wraps each merge with its source, an estimated size (for optional priority), and a FIFO tie-breaker.  
  Implements `Comparable` so smaller merges can be preferred when using a priority queue.  
  Calls back into the scheduler on completion for accurate accounting.

- **MergeTasksExecutorService**  
  Factory for a merge-only bounded pool. Choose a queue:  
  `PriorityBlockingQueue` (favor small), `LinkedBlockingQueue` (FIFO), or `SynchronousQueue` (no queue → immediate Caller-Runs).  
  Always uses `CallerRunsPolicy` to enforce backpressure.

- **IndexingMergeSharedExecutorService**  
  One shared pool for **indexing + merging** if you want a single hard cap on total indexer CPU. Under load, merges naturally slow indexing rather than storming the machine.

### Integration notes
- Use `MergeSource` (writer-agnostic) instead of casting to `IndexWriter`.  
- Add a small `IndexWriter.close()` hook to notify the scheduler (`onWriterClosed(source)`).

## Tests we added
1. Index → commit → search still works.  
2. With many writers, active merge threads stay within the pool cap (with tiny headroom).  
3. Per-writer counters/entries clear after writer close + merge completion.  
4. With a zero-length queue, merges run inline (no extra threads).  
5. Priority queues prefer small merges when enabled.  
6. Closing one writer doesn’t shut down an externally owned executor.  
7. Indexing+merge on the same pool coexist cleanly.  
8. Fairness sanity check with 20 writers: no starvation, all entries cleaned up.

## What this allows
- A single knob for global merge concurrency that scales across many writers  
- Simpler mental model and safer production behavior in multi-tenant setups  
- Option to cap **total** indexer CPU by sharing the pool with indexing

## Next Steps
- Add metrics (per-writer active merges, queue depth, inline counts)  
- Benchmark vs. CMS and shared-pool variants (throughput and tail latency)  
- Explore writer-aware throttling for stronger fairness guarantees  
- Document pool sizing and queue selection guidance

---

## Reflections
Open source felt huge and slow from the outside; working inside showed a **vibrant, responsive** community where small contributions happen all the time.  
- **Mentors:** Helped us ask better questions and engage the community early.  
- **Team:** Daily async comms across time zones; set norms, goals, helped clarify concepts, kep each other accountable.  
- **Surprises:** Dedication of contributors; codebase scale; deep integration with tools like Gradle.  
- **Skills:** Framed large problems, contributed with correct workflows, and built/benchmarked/tested changes in a team.

---

# Apache Lucene

![Lucene Logo](https://lucene.apache.org/theme/images/lucene/lucene_logo_green_300.png?v=0e493d7a)

Apache Lucene is a high-performance, full-featured text search engine library
written in Java.

[![Build Status](https://ci-builds.apache.org/job/Lucene/job/Lucene-Artifacts-main/badge/icon?subject=Lucene)](https://ci-builds.apache.org/job/Lucene/job/Lucene-Artifacts-main/)
[![Revved up by Develocity](https://img.shields.io/badge/Revved%20up%20by-Develocity-06A0CE?logo=Gradle&labelColor=02303A)](https://develocity.apache.org/scans?search.buildToolType=gradle&search.rootProjectNames=lucene-root)

## Online Documentation

This README file only contains basic setup instructions.  For more
comprehensive documentation, visit:

- Latest Releases: <https://lucene.apache.org/core/documentation.html>
- Nightly: <https://ci-builds.apache.org/job/Lucene/job/Lucene-Artifacts-main/javadoc/>
- New contributors should start by reading [Contributing Guide](./CONTRIBUTING.md)
- Build System Documentation: [help/](./help/)
- Migration Guide: [lucene/MIGRATE.md](./lucene/MIGRATE.md)

## Building

### Basic steps:

1. Install [OpenJDK 24](https://jdk.java.net/archive/).
2. Clone Lucene's git repository (or download the source distribution).
3. Run gradle launcher script (`gradlew`).

We'll assume that you know how to get and set up the JDK - if you don't, then we suggest starting at https://jdk.java.net/ and learning more about Java, before returning to this README.

## Contributing

Bug fixes, improvements and new features are always welcome!
Please review the [Contributing to Lucene
Guide](./CONTRIBUTING.md) for information on
contributing.

- Additional Developer Documentation: [dev-docs/](./dev-docs/)

## Discussion and Support

- [Users Mailing List](https://lucene.apache.org/core/discussion.html#java-user-list-java-userluceneapacheorg)
- [Developers Mailing List](https://lucene.apache.org/core/discussion.html#developer-lists)
- IRC: `#lucene` and `#lucene-dev` on freenode.net
