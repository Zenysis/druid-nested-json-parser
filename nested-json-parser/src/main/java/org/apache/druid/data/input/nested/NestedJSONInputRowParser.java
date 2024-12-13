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

// Copyright 2018 Zenysis Inc. All Rights Reserved.
// Author: stephen@zenysis.com (Stephen Ball)

package org.apache.druid.data.input.nested;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.JSONParseSpec;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.java.util.common.parsers.Parser;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NestedJSONInputRowParser implements InputRowParser<Object>
{
  private final JSONParseSpec parseSpec;
  private final List<PivotFieldSpec> pivotSpec;
  private final ObjectMapper mapper;
  private final Map<String, PivotFieldSpec> pivotFieldSpecs;
  private final Charset charset;

  private Parser<String, Object> parser;
  private CharBuffer chars;

  @JsonCreator
  public NestedJSONInputRowParser(
        @JsonProperty("parseSpec") JSONParseSpec parseSpec,
        @JsonProperty("pivotSpec") List<PivotFieldSpec> pivotSpec
  )
  {
    this.parseSpec = parseSpec;
    this.pivotSpec = pivotSpec;
    this.mapper = new ObjectMapper();

    this.pivotFieldSpecs = Maps.newLinkedHashMap();
    for (final PivotFieldSpec pivotFieldSpec : pivotSpec) {
      this.pivotFieldSpecs.put(
          pivotFieldSpec.getRowFieldName(),
          pivotFieldSpec
      );
    }
    // Maybe assert that pivotSpec's metrics are listed in the parseSpec?
    // Maybe assert that pivotSpec's dimension names exist in the
    // existing specified dimensions?
    Preconditions.checkArgument(
        parseSpec.getDimensionsSpec().hasFixedDimensions(),
        "Dimensions must be explicitly provided and cannot be missing."
    );

    // TODO(stephen): Derive this like StringInputRowParser does.
    this.charset = StandardCharsets.UTF_8;
  }

  // Imitate HaddopyStringInputRowParser.
  @Override
  public List<InputRow> parseBatch(Object input)
  {
    final Map<String, Object> theMap;
    if (input instanceof Text) {
      theMap = parseString(((Text) input).toString());
    } else if (input instanceof String) {
      theMap = parseString((String) input);
    } else {
      final ByteBuffer buffer;
      if (input instanceof ByteBuffer) {
        buffer = (ByteBuffer) input;
      } else if (input instanceof BytesWritable) {
        final BytesWritable valueBytes = (BytesWritable) input;
        buffer = ByteBuffer.wrap(valueBytes.getBytes(), 0,
                                 valueBytes.getLength());
      } else {
        throw new IAE(
          "Can't convert type [%s] to InputRow",
          input.getClass().getName()
        );
      }
      theMap = buildStringKeyMap(buffer);
    }
    return parseMap(theMap);
  }

  @JsonProperty
  @Override
  public ParseSpec getParseSpec()
  {
    return parseSpec;
  }

  @JsonProperty
  public List<PivotFieldSpec> getPivotSpec()
  {
    return pivotSpec;
  }

  @Override
  public NestedJSONInputRowParser withParseSpec(ParseSpec parseSpec)
  {
    return new NestedJSONInputRowParser((JSONParseSpec) parseSpec,
                                        getPivotSpec());
  }

  private Map<String, Object> buildStringKeyMap(ByteBuffer input)
  {
    int payloadSize = input.remaining();

    if (chars == null || chars.remaining() < payloadSize) {
      chars = CharBuffer.allocate(payloadSize);
    }

    final CoderResult coderResult = charset
        .newDecoder()
        .onMalformedInput(CodingErrorAction.REPLACE)
        .onUnmappableCharacter(CodingErrorAction.REPLACE)
        .decode(input, chars, true);

    Map<String, Object> theMap;
    if (coderResult.isUnderflow()) {
      chars.flip();
      try {
        theMap = parseString(chars.toString());
      }
      finally {
        chars.clear();
      }
    } else {
      throw new ParseException(null, "Failed with CoderResult[%s]", coderResult);
    }
    return theMap;
  }

  @Nullable
  private Map<String, Object> parseString(@Nullable String inputString)
  {
    if (parser == null) {
      // parser should be created when it is really used to avoid unnecessary
      // initialization of the underlying parseSpec.
      parser = new NestedJSONParser(parseSpec.getFlattenSpec(), null);
    }
    return parser.parseToMap(inputString);
  }

  @Nullable
  private List<InputRow> parseMap(@Nullable Map<String, Object> theMap)
  {
    if (theMap == null) {
      return null;
    }
    final List<InputRow> outputRows = new ArrayList<>();
    final Map<String, Object> baseMap = Maps.newLinkedHashMap();
    List<Map<String, Object>> pivotedFields = new ArrayList<>();
    pivotedFields.add(new HashMap<>());

    // Build a base map containing the common columns for all new InputRows.
    for (final String key : theMap.keySet()) {
      final Object value = theMap.get(key);
      if (pivotFieldSpecs.containsKey(key)) {
        if (value instanceof Map) {
          pivotedFields = buildCombinations((Map<String, Object>) value,
                                            pivotedFields,
                                            pivotFieldSpecs.get(key));
        } else {
          throw new IAE("Only Maps can be pivoted: %s", value);
        }
      } else {
        baseMap.put(key, value);
      }
    }

    // Copied from MapInputRowParser:parseBatch
    final DateTime time;
    try {
      time = parseSpec.getTimestampSpec().extractTimestamp(theMap);
      if (time == null) {
        final String input = theMap.toString();
        throw new NullPointerException(
            StringUtils.format(
                "Null timestamp in input: %s",
                input.length() < 100 ? input : input.substring(0, 100) + "..."
            )
        );
      }
    }
    catch (Exception e) {
      throw new ParseException(null, e, "Unparseable timestamp found! Event: %s",
                               theMap);
    }

    final long timestamp = time.getMillis();
    final List<String> dimensions = parseSpec.getDimensionsSpec()
                                             .getDimensionNames();

    for (final Map<String, Object> newMap : pivotedFields) {
      newMap.putAll(baseMap);
      outputRows.add(new MapBasedInputRow(timestamp, dimensions, newMap));
    }

    return outputRows;
  }

  private List<Map<String, Object>> buildCombinations(
        Map<String, Object> fieldsToPivot,
        List<Map<String, Object>> currentPivotedFields,
        PivotFieldSpec pivotFieldSpec
  )
  {
    final String dimension = pivotFieldSpec.getDimensionFieldName();
    final String metric = pivotFieldSpec.getMetricFieldName();
    final List<Map<String, Object>> output = new ArrayList<>();

    // Compute the cross of the existing pivoted fields with the new fields to
    // pivot.
    // NOTE(stephen): Is this behavior ever desired? Should this class just be
    // more strict and only allow one metric to be pivoted out?
    currentPivotedFields.forEach(m -> {
      fieldsToPivot.forEach((k, v) -> {
        if (!(v instanceof Number)) {
          throw new IAE("Metric value must be a number: %s", v);
        }
        final Map<String, Object> newFields = new HashMap<>(m);
        newFields.put(dimension, k);
        newFields.put(metric, v);
        output.add(newFields);
      });
    });

    return output;
  }
}
