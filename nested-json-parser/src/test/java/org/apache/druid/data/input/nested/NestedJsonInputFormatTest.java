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

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 */
public class NestedJsonInputFormatTest
{
  private static final String JSON_ROW =
      "{\"one\": \"foo\", \"nested\": {\"bar\": 1.0, \"baz\": 2.0}, " +
      "\"three\": \"qux\", \"timestamp\": \"1000\"}";

  private static final String JSON_SPEC = "{" +
      "\"type\": \"nestedJson\", " +
      "\"pivotSpec\": [{ " +
        "\"rowFieldName\": \"nested\", " +
        "\"dimensionFieldName\": \"two\", " +
        "\"metricFieldName\": \"val\" }]}";

  private static final ObjectMapper MAPPER = new DefaultObjectMapper();

  private InputFormat format;

  @Before
  public void setUp() throws Exception
  {
    for (Module jacksonModule : new NestedJSONParserModule().getJacksonModules()) {
      MAPPER.registerModule(jacksonModule);
    }

    format = MAPPER.readValue(
        MAPPER.writeValueAsBytes(
            MAPPER.readValue(JSON_SPEC, InputFormat.class)
        ),
        InputFormat.class
    );

    NullHandling.initializeForTests();
  }

  @Test
  public void testSerde() throws Exception
  {
    Assert.assertTrue(format instanceof NestedJsonInputFormat);
  }

  @Test
  public void testParseNestedRow() throws Exception
  {
    final ByteEntity source = new ByteEntity(StringUtils.toUtf8(JSON_ROW));
    final InputEntityReader reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("one", "two", "three"))),
            ImmutableList.of("val")
        ),
        source,
        null
    );

    final int numExpectedIterations = 2;
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      int numActualIterations = 0;
      while (iterator.hasNext()) {
        final InputRow inputRow = iterator.next();
        Assert.assertEquals(ImmutableList.of("one", "two", "three"),
                            inputRow.getDimensions());
        Assert.assertEquals(ImmutableList.of("foo"),
                            inputRow.getDimension("one"));
        Assert.assertEquals(ImmutableList.of("qux"),
                            inputRow.getDimension("three"));
        Assert.assertEquals(DateTimes.of("1000").getMillis(),
                            inputRow.getTimestampFromEpoch());

        // Test that the pivoted values for this row are correct.
        // HACK(stephen): It's not the cleanest way to do this, but it works.
        String expectedDimensionValue = numActualIterations == 0 ? "bar" : "baz";
        float expectedMetricValue = numActualIterations == 0 ? 1.0f : 2.0f;

        Assert.assertEquals(ImmutableList.of(expectedDimensionValue),
                            inputRow.getDimension("two"));
        Assert.assertEquals(expectedMetricValue,
                            inputRow.getMetric("val").floatValue(),
                            0.0f);
        numActualIterations++;
      }

      Assert.assertEquals(numExpectedIterations, numActualIterations);
    }
  }
}
