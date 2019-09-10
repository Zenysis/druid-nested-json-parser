/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.data.input.nested;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.indexer.HadoopDruidIndexerConfig;
import org.apache.druid.java.util.common.DateTimes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 */
public class NestedJSONInputRowParserTest
{
  private static final String JSON_ROW =
      "{\"one\": \"foo\", \"nested\": {\"bar\": 1.0, \"baz\": 2.0}, " +
      "\"three\": \"qux\", \"timestamp\": \"1000\"}";

  private static final String JSON_SPEC = "{" +
      "\"type\": \"nestedJson\", " +
      "\"parseSpec\": { " +
        "\"format\": \"json\", " +
        "\"timestampSpec\": { " +
          "\"column\": \"timestamp\", \"format\": \"iso\" }, " +
        "\"dimensionsSpec\": { " +
          "\"dimensions\": [\"one\", \"two\", \"three\"] } }, " +
      "\"pivotSpec\": [{ " +
        "\"rowFieldName\": \"nested\", " +
        "\"dimensionFieldName\": \"two\", " +
        "\"metricFieldName\": \"val\" }]}";

  private static final ObjectMapper MAPPER = HadoopDruidIndexerConfig.JSON_MAPPER;

  private InputRowParser parser;

  @Before
  public void setUp() throws Exception
  {
    parser = MAPPER.readValue(
        MAPPER.writeValueAsBytes(
            MAPPER.readValue(JSON_SPEC, InputRowParser.class)
        ),
        InputRowParser.class
    );
  }

  @Test
  public void testSerde() throws Exception
  {
    Assert.assertTrue(parser instanceof NestedJSONInputRowParser);
    Assert.assertEquals("timestamp", parser.getParseSpec()
                                           .getTimestampSpec()
                                           .getTimestampColumn());
  }

  @Test
  public void testParseBatchString() throws Exception
  {
    List<InputRow> inputRows = parser.parseBatch(JSON_ROW);
    Assert.assertEquals(2, inputRows.size());
  }

  @Test
  public void testParseBatchByteBuffer() throws Exception
  {
    List<InputRow> inputRows = parser.parseBatch(ByteBuffer.wrap(JSON_ROW.getBytes(StandardCharsets.UTF_8)));
    //Assert.assertEquals("hello world", MAPPER.writeValueAsString(inputRows));
    Assert.assertEquals(2, inputRows.size());
  }

  @Test
  public void testParseBatchOutput() throws Exception
  {
    List<InputRow> inputRows = parser.parseBatch(JSON_ROW);
    Assert.assertEquals(2, inputRows.size());
    for (InputRow inputRow : inputRows) {
      Assert.assertEquals(ImmutableList.of("one", "two", "three"),
                          inputRow.getDimensions());
      Assert.assertEquals(ImmutableList.of("foo"),
                          inputRow.getDimension("one"));
      Assert.assertEquals(ImmutableList.of("qux"),
                          inputRow.getDimension("three"));
      Assert.assertEquals(DateTimes.of("1000").getMillis(),
                          inputRow.getTimestampFromEpoch());
    }

    Assert.assertEquals(ImmutableList.of("bar"),
                        inputRows.get(0).getDimension("two"));
    Assert.assertEquals(1.0f, inputRows.get(0).getMetric("val").floatValue(),
                        0.0f);
    Assert.assertEquals(ImmutableList.of("baz"),
                        inputRows.get(1).getDimension("two"));
    Assert.assertEquals(2.0f, inputRows.get(1).getMetric("val").floatValue(),
                        0.0f);
  }
}
