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
import com.google.common.base.Preconditions;

import java.util.Objects;

public class PivotFieldSpec
{
  private final String rowFieldName;
  private final String dimensionFieldName;
  private final String metricFieldName;

  @JsonCreator
  public PivotFieldSpec(
      @JsonProperty("rowFieldName") String rowFieldName,
      @JsonProperty("dimensionFieldName") String dimensionFieldName,
      @JsonProperty("metricFieldName") String metricFieldName
  )
  {
    this.rowFieldName = Preconditions.checkNotNull(
      rowFieldName,
      "Missing 'rowFieldName' in field spec"
    );
    this.dimensionFieldName = Preconditions.checkNotNull(
      dimensionFieldName,
      "Missing 'dimensionFieldName' in field spec"
    );
    this.metricFieldName = Preconditions.checkNotNull(
      metricFieldName,
      "Missing 'metricFieldName' in field spec"
    );
  }

  @JsonProperty
  public String getRowFieldName()
  {
    return rowFieldName;
  }

  @JsonProperty
  public String getDimensionFieldName()
  {
    return dimensionFieldName;
  }

  @JsonProperty
  public String getMetricFieldName()
  {
    return metricFieldName;
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final PivotFieldSpec that = (PivotFieldSpec) o;
    return Objects.equals(rowFieldName, that.rowFieldName) &&
           Objects.equals(dimensionFieldName, that.dimensionFieldName) &&
           Objects.equals(metricFieldName, that.metricFieldName);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(rowFieldName, dimensionFieldName, metricFieldName);
  }

  @Override
  public String toString()
  {
    return "PivotFieldSpec{" +
           "rowFieldName='" + rowFieldName + '\'' +
           ", dimensionFieldName='" + dimensionFieldName + '\'' +
           ", metricFieldName='" + metricFieldName + '\'' +
           '}';
  }
}
