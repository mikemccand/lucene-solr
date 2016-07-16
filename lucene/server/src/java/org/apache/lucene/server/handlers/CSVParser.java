package org.apache.lucene.server.handlers;

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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.server.FieldDef;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

// TODO:
//   - multi-valued fields?
//   - escaping " , \n

class CSVParser {

  final static byte COMMA = (byte) ',';
  final static byte NEWLINE = (byte) '\n';
  
  final DataInput in;
  final byte[] buffer = new byte[1024];
  int bufferUpto;
  int bufferLimit;
  final FieldDef[] fields;
  private byte[] copyBuffer = new byte[1024];
  private int lineUpto = 0;
  public final IndexState indexState;

  public CSVParser(GlobalState state, DataInput in) throws IOException {
    this.in = in;

    // parse header and lookup fields:
    List<FieldDef> fieldsList = new ArrayList<>();
    StringBuilder curField = new StringBuilder();
    IndexState indexState = null;
    while (true) {
      if (bufferUpto == bufferLimit) {
        bufferLimit = in.readBytes(buffer, 0, buffer.length);
        if (bufferLimit == 0) {
          throw new IllegalArgumentException("hit end while parsing header");
        }
        bufferUpto = 0;
      }
      byte b = buffer[bufferUpto++];
      if (b == COMMA) {
        if (indexState == null) {
          throw new IllegalArgumentException("first line must be the index name");
        }
        fieldsList.add(indexState.getField(curField.toString()));
        curField.setLength(0);
      } else if (b == NEWLINE) {
        if (indexState == null) {
          String indexName = curField.toString();
          indexState = state.get(indexName);
        } else {
          fieldsList.add(indexState.getField(curField.toString()));
          break;
        }
      } else {
        // index and field names are simple ascii:
        curField.append((char) b);
      }
    }
    this.indexState = indexState;

    fields = fieldsList.toArray(new FieldDef[fieldsList.size()]);
  }

  public Document nextDoc() throws IOException {
    Document doc = new Document();

    int fieldUpto = 0;
    
    while (true) {
      if (bufferUpto == bufferLimit) {
        bufferLimit = in.readBytes(buffer, 0, buffer.length);
        if (bufferLimit == 0) {
          if (copyBufferUpto != 0) {
            throw new IllegalArgumentException("CSV input must end with a newline");
          }
          break;
        }
        bufferUpto = 0;
      }
      byte b = buffer[bufferUpto++];
      if (b == COMMA) {
        // nocommit need to handle escaping!
        DocValuesType dvType = fd.fieldType.docValuesType();            
        boolean stored = fd.fieldType.stored();
        
        switch(fields[fieldUpto].valueType) {
        case "atom":
          {
            String value = new String(copyBuffer, 0, copyBufferUpto, StandardCharsets.UTF8);
            if (fd.usePoints) {
              doc.add(new BinaryPoint(fd.name, new BytesRef(Arrays.copyOfRange(copyBuffer, 0, copyBufferUpto))));
            }
            if (stored || fd.fieldType.indexOptions() != IndexOptions.NONE) {
              doc.add(new MyField(fd.name, fd.fieldTypeNoDV, value));
            }
            if (dvType == DocValuesType.Sorted) {
              doc.add(new SortedDocValuesField(fd.name, new BytesRef(Arrays.copyOfRange(copyBuffer, 0, copyBufferUpto))));
            } else if (dvType == DocValuesType.SORTED_SET) {
              doc.add(new SortedSetDocValuesField(fd.name, new BytesRef(Arrays.copyOfRange(copyBuffer, 0, copyBufferUpto))));
            } else if (dvType == DocValuesType.BINARY) {
              doc.add(new BinaryDocValuesField(fd.name, new BytesRef(Arrays.copyOfRange(copyBuffer, 0, copyBufferUpto))));
            }
            break;
          }
        case "text":
          {
            String value = new String(copyBuffer, 0, copyBufferUpto, StandardCharsets.UTF8);
            doc.add(new MyField(fd.name, fd.fieldTypeNoDV, value));
            break;
          }
        case "int":
          {
            // TODO: bytes -> int directly
            int value = Integer.parseInt(new String(copyBuffer, 0, copyBufferUpto, StandardCharsets.US_ASCII));
            if (fd.usePoints) {
              doc.add(new IntPoint(fd.name, value));
            }
            if (stored) {
              doc.add(new StoredField(fd.name, value));
            }
            if (dvType == DocValuesType.NUMERIC) {
              doc.add(new IntDocValuesField(fd.name, value));
            } else if (dvType == DocValuesType.SORTED_NUMERIC) {
              doc.add(new SortedNumericDocValuesField(fd.name, value));
            }
            break;
          }
        case "long":
          {
            // TODO: bytes -> long directly
            long value = Long.parseLong(new String(copyBuffer, 0, copyBufferUpto, StandardCharsets.US_ASCII));
            if (fd.usePoints) {
              doc.add(new LongPoint(fd.name, value));
            }
            if (stored) {
              doc.add(new StoredField(fd.name, value));
            }
            if (dvType == DocValuesType.NUMERIC) {
              doc.add(new LongDocValuesField(fd.name, value));
            } else if (dvType == DocValuesType.SORTED_NUMERIC) {
              doc.add(new SortedNumericDocValuesField(fd.name, value));
            }
            break;
          }
        case "float":
          {
            // TODO: bytes -> float directly
            float value = Float.parseFloat(new String(copyBuffer, 0, copyBufferUpto, StandardCharsets.US_ASCII));
            if (fd.usePoints) {
              doc.add(new FloatPoint(fd.name, value));
            }
            if (stored) {
              doc.add(new StoredField(fd.name, value));
            }
            if (dvType == DocValuesType.NUMERIC) {
              doc.add(new FloatDocValuesField(fd.name, value));
            } else if (dvType == DocValuesType.SORTED_NUMERIC) {
              doc.add(new SortedNumericDocValuesField(fd.name, NumericUtils.floatToSortableInt(value)));
            }
            break;
          }
        case "double":
          {
            // TODO: bytes -> double directly
            double value = Double.parseDouble(new String(copyBuffer, 0, copyBufferUpto, StandardCharsets.US_ASCII));
            if (fd.usePoints) {
              doc.add(new DoublePoint(fd.name, value));
            }
            if (stored) {
              doc.add(new StoredField(fd.name, value));
            }
            if (dvType == DocValuesType.NUMERIC) {
              doc.add(new DoubleDocValuesField(fd.name, value));
            } else if (dvType == DocValuesType.SORTED_NUMERIC) {
              doc.add(new SortedNumericDocValuesField(fd.name, NumericUtils.doubleToSortableLong(value)));
            }
            break;
          }
        }
        fieldUpto++;
        if (fieldUpto >= fields.length) {
          throw new IllegalArgumentException("line " + lineUpto + " has too many fields");
        }
        copyBufferUpto = 0;
      } else if (b == NEWLINE) {
        lineUpto++;
        return doc;
      } else {
        if (copyBufferUpto == copyBuffer.length) {
          copyBuffer = ArrayUtil.grow(copyBuffer, copyBufferUpto);
        }
        copyBuffer[copyBufferUpto++] = b;
      }
    }

    return null;
  }
}
