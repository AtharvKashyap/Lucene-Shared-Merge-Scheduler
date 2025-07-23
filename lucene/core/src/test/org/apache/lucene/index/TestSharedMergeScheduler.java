package org.apache.lucene.index;

import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergeTaskWrapper;
import org.apache.lucene.index.SharedMergeScheduler;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.junit.Test;

import java.io.IOException;
import org.apache.lucene.index.IndexingMergeSharedExecutorService;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BooleanSupplier;

/** Unit-tests for {@link SharedMergeScheduler}. */
public class TestSharedMergeScheduler extends LuceneTestCase {

  /* ---------- helpers ---------- */

  private IndexWriter newWriter(Directory dir, SharedMergeScheduler sms) throws IOException {
    return new IndexWriter(
        dir,
        newIndexWriterConfig(new MockAnalyzer(random()))
            .setMergeScheduler(sms));
  }

  private static long countThreadsWithPrefix(String prefix) {
    return Thread.getAllStackTraces().keySet().stream()
        .filter(t -> t.getName().startsWith(prefix))
        .count();
  }

  private static void await(BooleanSupplier predicate, long timeoutMillis) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
    while (!predicate.getAsBoolean() && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertTrue("timeout waiting for predicate", predicate.getAsBoolean());
  }

  /* ---------- TEST 1   basic round-trip ---------- */

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

/* ---------- TEST 2  thread-cap respected during heavy merges ---------- */

@Test
public void testThreadCapRespected() throws Exception {
  final int POOL = 3;
  SharedMergeScheduler sms = SharedMergeScheduler.withDefault(POOL, 16, true);

  List<Directory> dirs = new ArrayList<>();
  List<IndexWriter> writers = new ArrayList<>();
  try {
    // each writer gets its own directory to avoid write.lock conflict
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

    await(() -> countThreadsWithPrefix("lucene-shared-merge-") <= POOL + 1, 5000);
  } finally {
    for (IndexWriter w : writers) w.close();
    for (Directory d : dirs) d.close();
    sms.close();
  }
}


  /* ---------- TEST 3  per-writer merge-count cleanup ---------- */

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
      w.flush();        // lots of segments
      w.close();        // writer hook calls onWriterClosed
      await(() -> sms.mergeCounts.isEmpty() && sms.closedSources.isEmpty(), 5000);
    } finally {
      sms.close();
      dir.close();
    }
  }

/* ---------- TEST 4  caller-runs fallback when queue full ---------- */

@Test
public void testCallerRunsFallback() throws Exception {
  // zero-length queue via SynchronousQueue
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
    w.flush();                    // triggers merge: queue is zero → caller-runs
    long thrAfter = countThreadsWithPrefix("lucene-shared-merge-");

    assertTrue("no extra merge thread expected", thrAfter - thrBefore <= 1);
    w.close();
  } finally {
    sms.close();
    dir.close();
  }
}
  /* ---------- TEST 5  small-merge priority comparator ---------- */

  @Test
  public void testMergeTaskWrapperComparable() {
    MergeTaskWrapper small = new MergeTaskWrapper(null, () -> {}, null, 100);
    MergeTaskWrapper big   = new MergeTaskWrapper(null, () -> {}, null, 1_000_000);
    assertTrue(small.compareTo(big) < 0);
    assertTrue(big.compareTo(small) > 0);
  }

  /* ---------- TEST 6  executor is not shut down by writer close ---------- */

  @Test
  public void testWriterCloseLeavesExecutorRunning() throws Exception {
    ExecutorService pool = MergeTasksExecutorService.newDefault(1, 8, true);
    SharedMergeScheduler sms = new SharedMergeScheduler(pool, false); // we own shutdown
    Directory dir = newDirectory();
    try {
      IndexWriter w = newWriter(dir, sms);
      Document d = new Document();
      d.add(new StringField("id", "a", Store.NO));
      w.addDocument(d);
      w.close();
      assertFalse("executor must still be open", pool.isShutdown());
    } finally {
      pool.shutdownNow();
      dir.close();
    }
  }

  /* ---------- TEST 7  index + merge share same pool ---------- */

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

  /* ---------- TEST 8  fairness: all writers finish merges ---------- */

  @Test
  public void testFairnessTwentyWriters() throws Exception {
    final int POOL = 4;
    SharedMergeScheduler sms = SharedMergeScheduler.withDefault(POOL, 32, true);
    List<Directory> dirs = new ArrayList<>();
    List<IndexWriter> writers = new ArrayList<>();
    try {
      // create 20 writers on independent ram dirs
      for (int i = 0; i < 20; i++) {
        Directory d = newDirectory();
        dirs.add(d);
        writers.add(newWriter(d, sms));
      }
      // index some docs
      for (IndexWriter w : writers) {
        for (int j = 0; j < 50; j++) {
          Document doc = new Document();
          doc.add(new StringField("id", UUID.randomUUID().toString(), Store.NO));
          w.addDocument(doc);
        }
        w.flush();
      }
      // close writers (schedules final merges)
      for (IndexWriter w : writers) w.close();

      // wait until executor terminates (sms.close polls awaitTermination)
      sms.close();
      assertTrue("all writer entries cleared",
                 sms.mergeCounts.isEmpty() && sms.closedSources.isEmpty());
    } finally {
      for (Directory d : dirs) d.close();
    }
  }
}
