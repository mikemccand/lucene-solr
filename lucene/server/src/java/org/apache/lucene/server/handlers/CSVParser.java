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

import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.BinaryPoint;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.server.FieldDef;
import org.apache.lucene.server.GlobalState;
import org.apache.lucene.server.IndexState;
import org.apache.lucene.store.DataInput;
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
  private int lineUpto;
  public final IndexState indexState;

  public CSVParser(GlobalState state, DataInput in) throws IOException {
    this.in = in;

    // parse header and lookup fields:
    List<FieldDef> fieldsList = new ArrayList<>();
    StringBuilder curField = new StringBuilder();
    IndexState indexState = null;
    while (true) {
      if (bufferUpto == bufferLimit) {
        try {
          in.readBytes(buffer, 0, buffer.length);
        } catch (EOFException eofe) {
          // nocommit this is bad!!!!  we will lose the last N docs here!!!
          break;
        }
        /*
        if (bufferLimit == 0) {
          throw new IllegalArgumentException("hit end while parsing header");
        }
        */
        bufferLimit = buffer.length;
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
          //System.out.println("INDEX: " + indexName);
          indexState = state.get(indexName);
          curField.setLength(0);
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
    int copyBufferUpto = 0;
    
    while (true) {
      if (bufferUpto == bufferLimit) {
        try {
          in.readBytes(buffer, 0, buffer.length);
        } catch (EOFException eofe) {
          // nocommit this is bad!!!!  we will lose the last N docs here!!!
          if (copyBufferUpto != 0) {
            throw new IllegalArgumentException("CSV input must end with a newline");
          }
          break;
        }
        /*
        if (bufferLimit == 0) {
          if (copyBufferUpto != 0) {
            throw new IllegalArgumentException("CSV input must end with a newline");
          }
          break;
        }
        */
        bufferLimit = buffer.length;
        bufferUpto = 0;
      }
      byte b = buffer[bufferUpto++];
      if (b == COMMA) {
        if (copyBufferUpto == 0) {
          // empty field
          fieldUpto++;
          continue;
        }
        // nocommit need to handle escaping!
        FieldDef fd = fields[fieldUpto];
        DocValuesType dvType = fd.fieldType.docValuesType();            
        boolean stored = fd.fieldType.stored();

        String s = new String(copyBuffer, 0, copyBufferUpto, StandardCharsets.US_ASCII);
        //System.out.println("FIELD " + fd.name + " -> " + s);
        
        switch(fd.valueType) {
        case "atom":
          {
            if (fd.usePoints) {
              doc.add(new BinaryPoint(fd.name, Arrays.copyOfRange(copyBuffer, 0, copyBufferUpto)));
            }
            if (stored || fd.fieldType.indexOptions() != IndexOptions.NONE) {
              doc.add(new AddDocumentHandler.MyField(fd.name, fd.fieldTypeNoDV, s));
            }
            if (dvType == DocValuesType.SORTED) {
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
            doc.add(new AddDocumentHandler.MyField(fd.name, fd.fieldTypeNoDV, s));
            break;
          }
        case "int":
          {
            // TODO: bytes -> int directly
            int value;
            try {
              value = Integer.parseInt(s);
            } catch (NumberFormatException nfe) {
              throw new IllegalArgumentException("line " + lineUpto + " field \"" + fd.name + "\": could not parse " + s + " as an int");
            }
            if (fd.usePoints) {
              doc.add(new IntPoint(fd.name, value));
            }
            if (stored) {
              doc.add(new StoredField(fd.name, value));
            }
            if (dvType == DocValuesType.NUMERIC) {
              doc.add(new NumericDocValuesField(fd.name, value));
            } else if (dvType == DocValuesType.SORTED_NUMERIC) {
              doc.add(new SortedNumericDocValuesField(fd.name, value));
            }
            break;
          }
        case "long":
          {
            // TODO: bytes -> long directly
            long value;
            try {
              value = Long.parseLong(s);
            } catch (NumberFormatException nfe) {
              throw new IllegalArgumentException("line " + lineUpto + " field \"" + fd.name + "\": could not parse " + s + " as a long");
            }
            if (fd.usePoints) {
              doc.add(new LongPoint(fd.name, value));
            }
            if (stored) {
              doc.add(new StoredField(fd.name, value));
            }
            if (dvType == DocValuesType.NUMERIC) {
              doc.add(new NumericDocValuesField(fd.name, value));
            } else if (dvType == DocValuesType.SORTED_NUMERIC) {
              doc.add(new SortedNumericDocValuesField(fd.name, value));
            }
            break;
          }
        case "float":
          {
            // TODO: bytes -> float directly
            float value;
            try {
              value = Float.parseFloat(s);
            } catch (NumberFormatException nfe) {
              throw new IllegalArgumentException("line " + lineUpto + " field \"" + fd.name + "\": could not parse " + s + " as a float");
            }
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
            double value;
            try {
              value = Double.parseDouble(s);
            } catch (NumberFormatException nfe) {
              throw new IllegalArgumentException("line " + lineUpto + " field \"" + fd.name + "\": could not parse " + s + " as a double");
            }
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
        if (fieldUpto+1 != fields.length) {
          throw new IllegalArgumentException("line " + lineUpto + " has " + (fieldUpto+1) + " fields, but expected " + fields.length);
        }
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
