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

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import org.apache.druid.initialization.DruidModule;

import java.util.Arrays;
import java.util.List;

public class NestedJSONParserModule implements DruidModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Arrays.asList(
        new SimpleModule("NestedJSONParserModule")
            .registerSubtypes(
                new NamedType(NestedJSONInputRowParser.class, "nestedJson"),
                new NamedType(NestedJsonInputFormat.class, "nestedJson")
            )
    );
  }

  @Override
  public void configure(Binder binder)
  {

  }
}
