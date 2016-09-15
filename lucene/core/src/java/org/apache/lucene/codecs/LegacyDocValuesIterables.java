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

package org.apache.lucene.codecs;

import java.io.IOException;
import java.util.Iterator;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;

import static org.apache.lucene.index.SortedSetDocValues.NO_MORE_ORDS;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/** Bridge helper methods for legacy codecs to map sorted doc values to iterables. */

public class LegacyDocValuesIterables {

  /** Converts {@link SortedDocValues} into an {@code Iterable&lt;BytesRef&gt;} for all the values.
   *
   * @deprecated Consume {@link SortedDocValues} instead. */
  @Deprecated
  public static Iterable<BytesRef> valuesIterable(final SortedDocValues values) {
    return new Iterable<BytesRef>() {
      @Override
      public Iterator<BytesRef> iterator() {
        return new Iterator<BytesRef>() {
          private int nextOrd;
    
          @Override
          public boolean hasNext() {
            return nextOrd < values.getValueCount();
          }

          @Override
          public BytesRef next() {
            return values.lookupOrd(nextOrd++);
          }
        };
      }
    };
  }

  /** Converts {@link SortedSetDocValues} into an {@code Iterable&lt;BytesRef&gt;} for all the values.
   *
   * @deprecated Consume {@link SortedSetDocValues} instead. */
  @Deprecated
  public static Iterable<BytesRef> valuesIterable(final SortedSetDocValues values) {
    return new Iterable<BytesRef>() {
      @Override
      public Iterator<BytesRef> iterator() {
        return new Iterator<BytesRef>() {
          private long nextOrd;
    
          @Override
          public boolean hasNext() {
            return nextOrd < values.getValueCount();
          }

          @Override
          public BytesRef next() {
            return values.lookupOrd(nextOrd++);
          }
        };
      }
    };
  }

  /** Converts {@link SortedDocValues} into the ord for each document as an {@code Iterable&lt;Number&gt;}.
   *
   * @deprecated Consume {@link SortedDocValues} instead. */
  @Deprecated
  public static Iterable<Number> sortedOrdIterable(final DocValuesProducer valuesProducer, FieldInfo fieldInfo, int maxDoc) {
    return new Iterable<Number>() {
      @Override
      public Iterator<Number> iterator() {

        final SortedDocValues values;
        try {
          values = valuesProducer.getSorted(fieldInfo);
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
        
        return new Iterator<Number>() {
          private int nextDocID;
    
          @Override
          public boolean hasNext() {
            return nextDocID < maxDoc;
          }

          @Override
          public Number next() {
            try {
              if (nextDocID > values.docID()) {
                values.nextDoc();
              }
              int result;
              if (nextDocID == values.docID()) {
                result = values.ordValue();
              } else {
                result = -1;
              }
              nextDocID++;
              return result;
            } catch (IOException ioe) {
              throw new RuntimeException(ioe);
            }
          }
        };
      }
    };
  }

  /** Converts number-of-ords per document from {@link SortedSetDocValues} into {@code Iterable&lt;Number&gt;}.
   *
   * @deprecated Consume {@link SortedSetDocValues} instead. */
  @Deprecated
  public static Iterable<Number> sortedSetOrdCountIterable(final DocValuesProducer valuesProducer, final FieldInfo fieldInfo, final int maxDoc) {

    return new Iterable<Number>() {

      @Override
      public Iterator<Number> iterator() {

        final SortedSetDocValues values;
        try {
          values = valuesProducer.getSortedSet(fieldInfo);
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }

        return new Iterator<Number>() {
          private int nextDocID;
          private int ordCount;
          
          @Override
          public boolean hasNext() {
            return nextDocID < maxDoc;
          }

          @Override
          public Number next() {
            try {
              if (nextDocID > values.docID()) {
                if (values.nextDoc() != NO_MORE_DOCS) {
                  ordCount = 0;
                  while (values.nextOrd() != NO_MORE_ORDS) {
                    ordCount++;
                  }
                }
              }
              int result;
              if (nextDocID == values.docID()) {
                result = ordCount;
              } else {
                result = 0;
              }
              nextDocID++;
              return result;
            } catch (IOException ioe) {
              throw new RuntimeException(ioe);
            }
          }
        };
      }
    };
  }

  /** Converts all concatenated ords (in docID order) from {@link SortedSetDocValues} into {@code Iterable&lt;Number&gt;}.
   *
   * @deprecated Consume {@link SortedSetDocValues} instead. */
  @Deprecated
  public static Iterable<Number> sortedSetOrdsIterable(final DocValuesProducer valuesProducer, final FieldInfo fieldInfo) {

    return new Iterable<Number>() {

      @Override
      public Iterator<Number> iterator() {

        final SortedSetDocValues values;
        try {
          values = valuesProducer.getSortedSet(fieldInfo);
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }

        return new Iterator<Number>() {
          private boolean nextIsSet;
          private long nextOrd;

          private void setNext() {
            try {
              if (nextIsSet == false) {
                if (values.docID() == -1) {
                  values.nextDoc();
                }
                while (true) {
                  if (values.docID() == NO_MORE_DOCS) {
                    nextOrd = -1;
                    break;
                  }
                  nextOrd = values.nextOrd();
                  if (nextOrd != -1) {
                    break;
                  }
                  values.nextDoc();
                }
                nextIsSet = true;
              }
            } catch (IOException ioe) {
              throw new RuntimeException(ioe);
            }
          }
          
          @Override
          public boolean hasNext() {
            setNext();
            return nextOrd != -1;
          }

          @Override
          public Number next() {
            setNext();
            assert nextOrd != -1;
            nextIsSet = false;
            return nextOrd;
          }
        };
      }
    };
  }

  /** Converts number-of-values per document from {@link SortedNumericDocValues} into {@code Iterable&lt;Number&gt;}.
   *
   * @deprecated Consume {@link SortedDocValues} instead. */
  @Deprecated
  public static Iterable<Number> sortedNumericToDocCount(final DocValuesProducer valuesProducer, final FieldInfo fieldInfo, int maxDoc) {
    return new Iterable<Number>() {

      @Override
      public Iterator<Number> iterator() {

        final SortedNumericDocValues values;
        try {
          values = valuesProducer.getSortedNumeric(fieldInfo);
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }

        return new Iterator<Number>() {
          private int nextDocID;
          private int nextCount;

          @Override
          public boolean hasNext() {
            return nextDocID < maxDoc;
          }

          @Override
          public Number next() {
            try {
              if (nextDocID > values.docID()) {
                values.nextDoc();
              }
              int result;
              if (nextDocID == values.docID()) {
                result = values.docValueCount();
              } else {
                result = 0;
              }
              nextDocID++;
              return result;
            } catch (IOException ioe) {
              throw new RuntimeException(ioe);
            }
          }
        };
      }
    };
  }

  /** Converts all concatenated values (in docID order) from {@link SortedNumericDocValues} into {@code Iterable&lt;Number&gt;}.
   *
   * @deprecated Consume {@link SortedDocValues} instead. */
  @Deprecated
  public static Iterable<Number> sortedNumericToValues(final DocValuesProducer valuesProducer, final FieldInfo fieldInfo) {
    return new Iterable<Number>() {

      @Override
      public Iterator<Number> iterator() {

        final SortedNumericDocValues values;
        try {
          values = valuesProducer.getSortedNumeric(fieldInfo);
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }

        return new Iterator<Number>() {
          private boolean nextIsSet;
          private int nextCount;
          private int upto;
          private long nextValue;

          private void setNext() {
            try {
              if (nextIsSet == false) {
                if (upto == nextCount) {
                  values.nextDoc();
                  if (values.docID() == NO_MORE_DOCS) {
                    nextCount = 0;
                    nextIsSet = false;
                    return;
                  } else {
                    nextCount = values.docValueCount();
                  }
                  upto = 0;
                }
                nextValue = values.nextValue();
                upto++;
                nextIsSet = true;
              }
            } catch (IOException ioe) {
              throw new RuntimeException(ioe);
            }
          }
          
          @Override
          public boolean hasNext() {
            setNext();
            return nextCount != 0;
          }

          @Override
          public Number next() {
            setNext();
            assert nextCount != 0;
            nextIsSet = false;
            return nextValue;
          }
        };
      }
    };
  }

  /** Converts norms into {@code Iterable&lt;Number&gt;}.
   *
   * @deprecated Consume {@link NumericDocValues} instead. */
  @Deprecated
  public static Iterable<Number> normsIterable(final FieldInfo field, final NormsProducer normsProducer, final int maxDoc) {

    return new Iterable<Number>() {

      @Override
      public Iterator<Number> iterator() {

        final NumericDocValues values;
        try {
          values = normsProducer.getNorms(field);
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
    
        return new Iterator<Number>() {
          private int docIDUpto = -1;

          @Override
          public boolean hasNext() {
            return docIDUpto+1 < maxDoc;
          }

          @Override
          public Number next() {
            docIDUpto++;
            if (docIDUpto > values.docID()) {
              try {
                values.nextDoc();
              } catch (IOException ioe) {
                throw new RuntimeException(ioe);
              }
            }
            Number result;
            if (docIDUpto == values.docID()) {
              result = values.longValue();
            } else {
              // Unlike NumericDocValues, norms should return for missing values:
              result = 0;
            }
            return result;
          }
        };
      }
    };
  }

  /** Converts values from {@link BinaryDocValues} into {@code Iterable&lt;BytesRef&gt;}.
   *
   * @deprecated Consume {@link BinaryDocValues} instead. */
  @Deprecated
  public static Iterable<BytesRef> binaryIterable(final FieldInfo field, final DocValuesProducer valuesProducer, final int maxDoc) {
    return new Iterable<BytesRef>() {
      @Override
      public Iterator<BytesRef> iterator() {

        final BinaryDocValues values;
        try {
          values = valuesProducer.getBinary(field);
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }

        return new Iterator<BytesRef>() {
          private int docIDUpto = -1;

          @Override
          public boolean hasNext() {
            return docIDUpto+1 < maxDoc;
          }

          @Override
          public BytesRef next() {
            docIDUpto++;
            if (docIDUpto > values.docID()) {
              try {
                values.nextDoc();
              } catch (IOException ioe) {
                throw new RuntimeException(ioe);
              }
            }
            BytesRef result;
            if (docIDUpto == values.docID()) {
              result = values.binaryValue();
            } else {
              result = null;
            }
            return result;
          }
        };
      }
    };
  }

  /** Converts values from {@link NumericDocValues} into {@code Iterable&lt;Number&gt;}.
   *
   * @deprecated Consume {@link NumericDocValues} instead. */
  @Deprecated
  public static Iterable<Number> numericIterable(final FieldInfo field, final DocValuesProducer valuesProducer, final int maxDoc) {
    return new Iterable<Number>() {
      @Override
      public Iterator<Number> iterator() {

        final NumericDocValues values;
        try {
          values = valuesProducer.getNumeric(field);
          // nocommit don't do this here; make it like the StupidBinary one:
          values.nextDoc();
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
    
        return new Iterator<Number>() {
          private int docIDUpto;

          @Override
          public boolean hasNext() {
            return docIDUpto < maxDoc;
          }

          @Override
          public Number next() {
            Long result;
            if (docIDUpto == values.docID()) {
              result = Long.valueOf(values.longValue());
              try {
                values.nextDoc();
              } catch (IOException ioe) {
                throw new RuntimeException(ioe);
              }
            } else {
              result = null;
            }
            docIDUpto++;
            return result;
          }
        };
      }
    };
  }
}
