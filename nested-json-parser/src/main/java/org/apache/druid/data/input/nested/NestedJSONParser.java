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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.common.parsers.ObjectFlattener;
import org.apache.druid.java.util.common.parsers.ObjectFlatteners;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.java.util.common.parsers.Parser;

import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Customized version of JSONPathParser
 */
public class NestedJSONParser implements Parser<String, Object>
{
  private final ObjectMapper mapper;
  private final CharsetEncoder enc = StandardCharsets.UTF_8.newEncoder();
  private final ObjectFlattener<JsonNode> flattener;

  /**
   * Constructor
   *
   * @param flattenSpec Provide a path spec for flattening and field discovery.
   * @param mapper      Optionally provide an ObjectMapper, used by the parser for reading the input JSON.
   */
  public NestedJSONParser(JSONPathSpec flattenSpec, ObjectMapper mapper)
  {
    this.mapper = mapper == null ? new ObjectMapper() : mapper;
    this.flattener = ObjectFlatteners.create(flattenSpec,
                                             new NestedJSONFlattenerMaker());
  }

  @Override
  public List<String> getFieldNames()
  {
    return null;
  }

  @Override
  public void setFieldNames(Iterable<String> fieldNames)
  {
  }

  /**
   * @param input JSON string. The root must be a JSON object, not an array.
   *              e.g., {"valid": "true"} and {"valid":[1,2,3]} are supported
   *              but [{"invalid": "true"}] and [1,2,3] are not. Nested flat
   *              JSON objects are supported {"nested": {"one": 1, "two": 2}}.
   *
   * @return A map of field names and values
   */
  @Override
  public Map<String, Object> parseToMap(String input)
  {
    try {
      JsonNode document = mapper.readValue(input, JsonNode.class);
      return flattener.flatten(document);
    }
    catch (Exception e) {
      throw new ParseException(null, e, "Unable to parse row [%s]", input);
    }
  }
}
