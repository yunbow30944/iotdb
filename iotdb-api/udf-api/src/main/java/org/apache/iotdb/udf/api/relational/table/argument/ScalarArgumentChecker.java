/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.udf.api.relational.table.argument;

import java.util.function.Function;

public class ScalarArgumentChecker {
  public static Function<Object, String> POSITIVE_LONG_CHECKER =
      (value) -> {
        if (value instanceof Long && (Long) value > 0) {
          return null;
        }
        return "should be a positive value";
      };

  public static Function<Object, String> NON_NEGATIVE_DOUBLE_CHECKER =
      (value) -> {
        if (value instanceof Double && (Double) value >= 0) {
          return null;
        }
        return "should be a non-negative value";
      };
}
