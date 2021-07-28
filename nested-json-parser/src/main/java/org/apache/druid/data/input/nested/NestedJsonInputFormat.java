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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.NestedInputFormat;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class NestedJsonInputFormat extends NestedInputFormat
{
  private final ObjectMapper objectMapper;
  private final List<PivotFieldSpec> pivotSpec;
  private final Map<String, PivotFieldSpec> pivotFieldSpecs;

  @JsonCreator
  public NestedJsonInputFormat(
      @JsonProperty("pivotSpec") List<PivotFieldSpec> pivotSpec
  )
  {
    super(null);
    this.objectMapper = new ObjectMapper();
    this.pivotSpec = pivotSpec;
    this.pivotFieldSpecs = Maps.newLinkedHashMap();
    for (final PivotFieldSpec pivotFieldSpec : pivotSpec) {
      this.pivotFieldSpecs.put(
          pivotFieldSpec.getRowFieldName(),
          pivotFieldSpec
      );
    }
  }

  @JsonProperty
  public List<PivotFieldSpec> getPivotSpec()
  {
    return pivotSpec;
  }

  @Override
  public boolean isSplittable()
  {
    return false;
  }

  @Override
  public InputEntityReader createReader(InputRowSchema inputRowSchema, InputEntity source, File temporaryDirectory)
  {
    return new NestedJsonLineReader(inputRowSchema, source, objectMapper, pivotFieldSpecs);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    NestedJsonInputFormat that = (NestedJsonInputFormat) o;
    return Objects.equals(pivotFieldSpecs, that.pivotFieldSpecs);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), pivotFieldSpecs);
  }
}
