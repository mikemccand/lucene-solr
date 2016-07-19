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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

// javac -cp ../build/core/classes/java IndexTaxis.java ; java -cp ../build/core/classes/java:. IndexTaxis /b/taxisjava 6 /lucenedata/nyc-taxi-data/alltaxis.csv.blocks 

public class IndexTaxis {

  private static final int NEWLINE = (byte) '\n';
  private static final int COMMA = (byte) ',';
  private static final byte[] header = new byte[128];

  static final long startNS = System.nanoTime();

  private static class Chunk {
    public final byte[] bytes;
    public final int docCount;

    public Chunk(byte[] bytes, int docCount) {
      this.bytes = bytes;
      this.docCount = docCount;
    }
  }

  private synchronized static Chunk readChunk(BufferedInputStream docs) throws IOException {
    int count = docs.read(header, 0, header.length);
    if (count == -1) {
      // end
      return null;
    }

    int upto = 0;
    while (upto < header.length) {
      if (header[upto] == NEWLINE) {
        break;
      }
      upto++;
    }
    if (upto == header.length) {
      throw new AssertionError();
    }
    String[] parts = new String(header, 0, upto, StandardCharsets.UTF_8).split(" ");
    if (parts.length != 2) {
      throw new AssertionError();
    }
    int byteCount = Integer.parseInt(parts[0]);
    int docCount = Integer.parseInt(parts[1]);
    byte[] chunk = new byte[byteCount];
    int fragment = header.length-upto-1;
    System.arraycopy(header, upto+1, chunk, 0, fragment);
    count = docs.read(chunk, fragment, chunk.length - fragment);
    if (count != chunk.length - fragment) {
      throw new AssertionError();
    }
    return new Chunk(chunk, docCount);
  }

  static void addOneField(Document doc, String fieldName, String rawValue) {
    switch(fieldName) {
    case "vendor_id":
    case "cab_color":
    case "payment_type":
    case "trip_type":
    case "rate_code":
    case "store_and_fwd_flag":
      doc.add(new StringField(fieldName, rawValue, Field.Store.NO));
      break;
    case "vendor_name":
      doc.add(new TextField(fieldName, rawValue, Field.Store.NO));
      break;
    case "pick_up_date_time":
    case "drop_off_date_time":
      doc.add(new LongPoint(fieldName, Long.parseLong(rawValue)));
      break;
    case "passenger_count":
      doc.add(new IntPoint(fieldName, Integer.parseInt(rawValue)));
      break;
    case "trip_distance":
    case "pick_up_lat":
    case "pick_up_lon":
    case "drop_off_lat":
    case "drop_off_lon":
    case "fare_amount":
    case "surcharge":
    case "mta_tax":
    case "extra":
    case "ehail_fee":
    case "improvement_surcharge":
    case "tip_amount":
    case "tolls_amount":
    case "total_amount":
      try {
        doc.add(new DoublePoint(fieldName, Double.parseDouble(rawValue)));
      } catch (NumberFormatException nfe) {
        System.out.println("WARNING: failed to parse \"" + rawValue + "\" as double for field \"" + fieldName + "\"");
      }
      break;
    default:
      throw new AssertionError("failed to handle field \"" + fieldName + "\"");
    }
  }

  /** Index all documents contained in one chunk */
  static void indexOneChunk(String[] fields, Chunk chunk, IndexWriter w, int startByte, AtomicInteger docCounter, AtomicLong bytesCounter) throws IOException {

    Document doc = new Document();
    byte[] bytes = chunk.bytes;
    if (bytes[bytes.length-1] != NEWLINE) {
      throw new AssertionError();
    }
    int lastFieldStart = startByte;
    int lastLineStart = startByte;
    int fieldUpto = 0;
    for(int i=startByte;i<bytes.length;i++) {
      byte b = bytes[i];
      if (b == NEWLINE || b == COMMA) {
        if (i > lastFieldStart) {
          String s = new String(bytes, lastFieldStart, i-lastFieldStart, StandardCharsets.UTF_8);
          addOneField(doc, fields[fieldUpto], s);
        }
        if (b == NEWLINE) {
          if (fieldUpto != fields.length-1) {
            throw new AssertionError();
          }
          w.addDocument(doc);
          int x = docCounter.incrementAndGet();
          long y = bytesCounter.addAndGet(i - lastLineStart);
          if (x % 100000 == 0) {
            double sec = (System.nanoTime() - startNS)/1000000000.0;
            System.out.println(String.format(Locale.ROOT, "%.1f sec: %d docs; %.1f docs/sec; %.1f MB/sec", sec, x, x/sec, (y/1024./1024.)/sec));
          }
          doc = new Document();
          fieldUpto = 0;
          lastLineStart = i+1;
        } else {
          fieldUpto++;
        }
        lastFieldStart = i+1;
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Path indexPath = Paths.get(args[0]);
    Directory dir = FSDirectory.open(indexPath);
    int threadCount = Integer.parseInt(args[1]);
    Path docsPath = Paths.get(args[2]);

    IndexWriterConfig iwc = new IndexWriterConfig();
    iwc.setRAMBufferSizeMB(1024.);

    final IndexWriter w = new IndexWriter(dir, iwc);

    BufferedInputStream docs = new BufferedInputStream(Files.newInputStream(docsPath, StandardOpenOption.READ));

    final Chunk firstChunk = readChunk(docs);

    // parse the header fields
    List<String> fieldsList = new ArrayList<>();
    int lastFieldStart = 0;
    int chunkStartByte = 0;
    for(int i=0;i<firstChunk.bytes.length;i++) {
      byte b = firstChunk.bytes[i];
      if (b == NEWLINE) {
        fieldsList.add(new String(firstChunk.bytes, lastFieldStart, i-lastFieldStart, StandardCharsets.UTF_8));
        chunkStartByte = i+1;
        break;
      } else if (b == COMMA) {
        fieldsList.add(new String(firstChunk.bytes, lastFieldStart, i-lastFieldStart, StandardCharsets.UTF_8));
        lastFieldStart = i+1;
      }
    }

    final int finalStartByte = chunkStartByte;

    final String[] fields = fieldsList.toArray(new String[fieldsList.size()]);

    Thread[] threads = new Thread[threadCount];

    final AtomicInteger docCounter = new AtomicInteger();
    final AtomicLong bytesCounter = new AtomicLong();

    for(int i=0;i<threadCount;i++) {
      final int threadID = i;
      threads[i] = new Thread() {
          @Override
          public void run() {
            try {
              _run();
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }

          private void _run() throws IOException {
            if (threadID == 0) {
              indexOneChunk(fields, firstChunk, w, finalStartByte, docCounter, bytesCounter);
            }
            while (true) {
              Chunk chunk = readChunk(docs);
              if (chunk == null) {
                break;
              }
              indexOneChunk(fields, chunk, w, 0, docCounter, bytesCounter);
            }
          }
        };
      threads[i].start();
    }

    for(int i=0;i<threadCount;i++) {
      threads[i].join();
    }
    System.out.println("Indexing done; now close");

    w.close();
    docs.close();
  }
}
