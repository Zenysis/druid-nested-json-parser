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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.TextReader;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.common.parsers.ObjectFlattener;
import org.apache.druid.java.util.common.parsers.ObjectFlatteners;
import org.apache.druid.java.util.common.parsers.ParseException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NestedJsonLineReader extends TextReader
{
  private final ObjectFlattener<JsonNode> flattener;
  private final ObjectMapper mapper;
  private final Map<String, PivotFieldSpec> pivotFieldSpecs;

  NestedJsonLineReader(
      InputRowSchema inputRowSchema,
      InputEntity source,
      ObjectMapper mapper,
      Map<String, PivotFieldSpec> pivotFieldSpecs
  )
  {
    super(inputRowSchema, source);
    this.flattener = ObjectFlatteners.create(JSONPathSpec.DEFAULT, new NestedJSONFlattenerMaker());
    this.mapper = mapper;
    this.pivotFieldSpecs = pivotFieldSpecs;
  }

  @Override
  public List<InputRow> parseInputRows(String line) throws IOException, ParseException
  {
    final JsonNode document = mapper.readValue(line, JsonNode.class);
    final Map<String, Object> flattened = flattener.flatten(document);
    return parseMap(flattened, getInputRowSchema());
  }

  @Override
  public List<Map<String, Object>> toMap(String intermediateRow) throws IOException
  {
    //noinspection unchecked
    return Collections.singletonList(mapper.readValue(intermediateRow, Map.class));
  }

  @Override
  public int getNumHeaderLinesToSkip()
  {
    return 0;
  }

  @Override
  public boolean needsToProcessHeaderLine()
  {
    return false;
  }

  @Override
  public void processHeaderLine(String line)
  {
    // do nothing
  }

  private List<InputRow> parseMap(
      Map<String, Object> theMap,
      InputRowSchema inputRowSchema
  )
  {
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

    for (final Map<String, Object> newMap : pivotedFields) {
      newMap.putAll(baseMap);
      outputRows.add(MapInputRowParser.parse(inputRowSchema, newMap));
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
