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
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BooleanSupplier;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.Test;

/** Unit-tests for {@link SharedMergeScheduler}. */
public class TestSharedMergeScheduler extends LuceneTestCase {

  /* ---------- small helper ---------- */

  /**
   * Wait until {@code predicate.getAsBoolean()} is true or 10 s elapse. No Thread.sleep is used
   * (forbidden); we spin with {@code Thread.onSpinWait()}.
   */
  private static void await(BooleanSupplier predicate) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (!predicate.getAsBoolean() && System.nanoTime() < deadline) {
      Thread.onSpinWait();
    }
    assertTrue("timeout waiting for predicate", predicate.getAsBoolean());
  }

  private IndexWriter newWriter(Directory dir, SharedMergeScheduler sms) throws IOException {
    return new IndexWriter(
        dir, newIndexWriterConfig(new MockAnalyzer(random())).setMergeScheduler(sms));
  }

  private static long countThreadsWithPrefix(String prefix) {
    return Thread.getAllStackTraces().keySet().stream()
        .filter(t -> t.getName().startsWith(prefix))
        .count();
  }

  /* ---------- TEST 1  basic round-trip ---------- */

  @Test
  public void testBasicIndexingAndSearch() throws Exception {
    SharedMergeScheduler sms = SharedMergeScheduler.withDefault(2, 8, true);
    Directory dir = newDirectory();
    try {
      try (IndexWriter w = newWriter(dir, sms)) {
        for (int i = 0; i < 10; i++) {
          Document d = new Document();
          d.add(new StringField("id", Integer.toString(i), Store.YES));
          w.addDocument(d);
        }
      }
      try (DirectoryReader rd = DirectoryReader.open(dir)) {
        assertEquals("all docs visible", 10, rd.numDocs());
      }
    } finally {
      sms.close();
      dir.close();
    }
  }

  /* ---------- TEST 2  thread-cap respected ---------- */

  @Test
  public void testThreadCapRespected() throws Exception {
    final int POOL = 3;
    SharedMergeScheduler sms = SharedMergeScheduler.withDefault(POOL, 16, true);

    List<Directory> dirs = new ArrayList<>();
    List<IndexWriter> writers = new ArrayList<>();
    try {
      for (int i = 0; i < 8; i++) {
        Directory d = newDirectory();
        dirs.add(d);
        writers.add(newWriter(d, sms));
      }

      for (int round = 0; round < 50; round++) {
        for (IndexWriter w : writers) {
          Document doc = new Document();
          doc.add(new StringField("id", UUID.randomUUID().toString(), Store.NO));
          w.addDocument(doc);
        }
      }

      await(() -> countThreadsWithPrefix("lucene-shared-merge-") <= POOL + 1);
    } finally {
      for (IndexWriter w : writers) w.close();
      for (Directory d : dirs) d.close();
      sms.close();
    }
  }

  /* ---------- TEST 3  per-writer cleanup ---------- */

  @Test
  public void testPerWriterCountsCleanedOnClose() throws Exception {
    SharedMergeScheduler sms = SharedMergeScheduler.withDefault(1, 8, true);
    Directory dir = newDirectory();
    try {
      IndexWriter w = newWriter(dir, sms);
      for (int i = 0; i < 100; i++) {
        Document d = new Document();
        d.add(new StringField("id", Integer.toString(i), Store.NO));
        w.addDocument(d);
      }
      w.flush();
      w.close();
      await(() -> sms.mergeCounts.isEmpty() && sms.closedSources.isEmpty());
    } finally {
      sms.close();
      dir.close();
    }
  }

  /* ---------- TEST 4  caller-runs fallback ---------- */

  @Test
  public void testCallerRunsFallback() throws Exception {
    SharedMergeScheduler sms = SharedMergeScheduler.withDefault(1, 0, true);
    Directory dir = newDirectory();
    try {
      IndexWriter w = newWriter(dir, sms);
      long thrBefore = countThreadsWithPrefix("lucene-shared-merge-");

      for (int i = 0; i < 500; i++) {
        Document d = new Document();
        d.add(new StringField("id", "x", Store.NO));
        w.addDocument(d);
      }
      w.flush();
      long thrAfter = countThreadsWithPrefix("lucene-shared-merge-");

      assertTrue("no extra merge thread expected", thrAfter - thrBefore <= 1);
      w.close();
    } finally {
      sms.close();
      dir.close();
    }
  }

  /* ---------- TEST 5  comparator ---------- */

  @Test
  public void testMergeTaskWrapperComparable() {
    MergeTaskWrapper small = new MergeTaskWrapper(null, () -> {}, null, 100);
    MergeTaskWrapper big = new MergeTaskWrapper(null, () -> {}, null, 1_000_000);
    assertTrue(small.compareTo(big) < 0);
    assertTrue(big.compareTo(small) > 0);
  }

  /* ---------- TEST 6  executor survives writer close ---------- */

  @Test
  public void testWriterCloseLeavesExecutorRunning() throws Exception {
    ExecutorService pool = MergeTasksExecutorService.newDefault(1, 8, true);
    SharedMergeScheduler sms = new SharedMergeScheduler(pool, false);
    Directory dir = newDirectory();
    try {
      IndexWriter w = newWriter(dir, sms);
      Document d = new Document();
      d.add(new StringField("id", "a", Store.NO));
      w.addDocument(d);
      w.close();
      assertFalse(pool.isShutdown());
    } finally {
      pool.shutdownNow();
      dir.close();
    }
  }

  /* ---------- TEST 7  shared pool ---------- */

  @Test
  public void testSharedPoolForIndexingAndMerging() throws Exception {
    ExecutorService shared = IndexingMergeSharedExecutorService.newSharedPool(2, 16, true);
    SharedMergeScheduler sms = new SharedMergeScheduler(shared, false);
    Directory dir = newDirectory();
    try {
      try (IndexWriter w = newWriter(dir, sms)) {
        for (int i = 0; i < 100; i++) {
          Document d = new Document();
          d.add(new StringField("id", Integer.toString(i), Store.YES));
          w.addDocument(d);
        }
      }
    } finally {
      shared.shutdownNow();
      dir.close();
    }
  }

  /* ---------- TEST 8  fairness across 20 writers ---------- */

  @Test
  public void testFairnessTwentyWriters() throws Exception {
    final int POOL = 4;
    SharedMergeScheduler sms = SharedMergeScheduler.withDefault(POOL, 32, true);
    List<Directory> dirs = new ArrayList<>();
    List<IndexWriter> writers = new ArrayList<>();
    try {
      for (int i = 0; i < 20; i++) {
        Directory d = newDirectory();
        dirs.add(d);
        writers.add(newWriter(d, sms));
      }
      for (IndexWriter w : writers) {
        for (int j = 0; j < 50; j++) {
          Document doc = new Document();
          doc.add(new StringField("id", UUID.randomUUID().toString(), Store.NO));
          w.addDocument(doc);
        }
        w.flush();
      }
      for (IndexWriter w : writers) w.close();

      sms.close();
      assertTrue(sms.mergeCounts.isEmpty() && sms.closedSources.isEmpty());
    } finally {
      for (Directory d : dirs) d.close();
    }
  }
}
