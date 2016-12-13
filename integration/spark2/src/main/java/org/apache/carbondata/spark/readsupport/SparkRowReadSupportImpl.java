/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.carbondata.spark.readsupport;

import java.math.BigDecimal;
import java.sql.Timestamp;

import org.apache.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.apache.carbondata.core.carbon.metadata.encoder.Encoding;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.hadoop.readsupport.impl.AbstractDictionaryDecodedReadSupport;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.Decimal;

public class SparkRowReadSupportImpl extends AbstractDictionaryDecodedReadSupport<Row> {

  @Override public void initialize(CarbonColumn[] carbonColumns,
      AbsoluteTableIdentifier absoluteTableIdentifier) {
    super.initialize(carbonColumns, absoluteTableIdentifier);
    //can initialize and generate schema here.
  }

  @Override public Row readRow(Object[] data) {
    for (int i = 0; i < dictionaries.length; i++) {
      if (dictionaries[i] == null) {
        if (carbonColumns[i].hasEncoding(Encoding.DIRECT_DICTIONARY)) {
          //convert the long to timestamp in case of direct dictionary column
          if (DataType.TIMESTAMP == carbonColumns[i].getDataType()) {
            data[i] = new Timestamp((long) data[i] / 1000);
          }
        } else if(dataTypes[i].equals(DataType.INT)) {
          data[i] = ((Long)(data[i])).intValue();
        } else if(dataTypes[i].equals(DataType.SHORT)) {
          data[i] = ((Long)(data[i])).shortValue();
        }
      } else if (carbonColumns[i].getDataType().equals(DataType.DECIMAL)) {
        data[i] = Decimal.apply((BigDecimal) data[i]);
      } else if (carbonColumns[i].getDataType().equals(DataType.ARRAY)) {
        data[i] = new GenericArrayData(data[i]);
      } else if (carbonColumns[i].getDataType().equals(DataType.STRUCT)) {
        data[i] = new GenericInternalRow((Object[]) data[i]);
      }
    }
    return new GenericRow(data);
  }
}
