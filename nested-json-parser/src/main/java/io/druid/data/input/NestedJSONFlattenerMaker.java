/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// Copyright 2018 Zenysis Inc. All Rights Reserved.
// Author: stephen@zenysis.com (Stephen Ball)

package io.druid.data.input.nested;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.FluentIterable;

import io.druid.java.util.common.parsers.JSONFlattenerMaker;

import java.util.Iterator;
import java.util.Map;

public class NestedJSONFlattenerMaker extends JSONFlattenerMaker
{
  @Override
  public Iterable<String> discoverRootFields(final JsonNode obj)
  {
    return FluentIterable.from(() -> obj.fields())
                         .filter(
                             entry -> {
                               final JsonNode val = entry.getValue();
                               return !(
                                val.isNull() ||
                                (val.isObject() && !isObjectAllowed(val)) ||
                                (val.isArray() && !isFlatList(val)));
                             }
                         )
                         .transform(Map.Entry::getKey);
  }

  private boolean isObjectAllowed(JsonNode obj)
  {
    for (Iterator<JsonNode> it = obj.elements(); it.hasNext();) {
      final JsonNode val = it.next();
      if (val.isNull() || val.isObject() || val.isArray()) {
        return false;
      }
    }
    return true;
  }

  // Copied from JSONFlattenerMaker
  private boolean isFlatList(JsonNode list)
  {
    for (JsonNode obj : list) {
      if (obj.isObject() || obj.isArray()) {
        return false;
      }
    }
    return true;
  }
}
