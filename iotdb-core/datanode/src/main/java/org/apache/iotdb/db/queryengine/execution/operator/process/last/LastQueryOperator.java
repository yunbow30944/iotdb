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

package org.apache.iotdb.db.queryengine.execution.operator.process.last;

import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.ProcessOperator;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.Futures.successfulAsList;

// collect all last query result in the same data region and there is no order guarantee
public class LastQueryOperator implements ProcessOperator {

  private static final int MAX_DETECT_COUNT =
      TSFileDescriptor.getInstance().getConfig().getMaxTsBlockLineNumber();

  private static final int DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES =
      TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(LastQueryOperator.class);

  private final OperatorContext operatorContext;

  private final List<Operator> children;

  private final int inputOperatorsCount;

  private int currentIndex;

  private TsBlockBuilder tsBlockBuilder;

  public LastQueryOperator(
      OperatorContext operatorContext, List<Operator> children, TsBlockBuilder builder) {
    this.operatorContext = operatorContext;
    this.children = children;
    this.inputOperatorsCount = children.size();
    this.currentIndex = 0;
    this.tsBlockBuilder = builder;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    if (currentIndex < inputOperatorsCount) {
      int endIndex = getEndIndex();
      List<ListenableFuture<?>> listenableFutures = new ArrayList<>();
      for (int i = currentIndex; i < endIndex; i++) {
        ListenableFuture<?> blocked = children.get(i).isBlocked();
        if (!blocked.isDone()) {
          listenableFutures.add(blocked);
        }
      }
      return listenableFutures.isEmpty() ? NOT_BLOCKED : successfulAsList(listenableFutures);
    } else {
      return Futures.immediateVoidFuture();
    }
  }

  @Override
  public TsBlock next() throws Exception {

    // we have consumed up data from children Operator, just return all remaining cached data in
    // tsBlockBuilder
    if (currentIndex >= inputOperatorsCount) {
      TsBlock res = tsBlockBuilder.build();
      tsBlockBuilder.reset();
      return res;
    }

    // start stopwatch
    long maxRuntime = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
    long start = System.nanoTime();

    int endIndex = getEndIndex();

    while ((System.nanoTime() - start < maxRuntime)
        && (currentIndex < endIndex)
        && !tsBlockBuilder.isFull()) {
      if (children.get(currentIndex).hasNextWithTimer()) {
        TsBlock tsBlock = children.get(currentIndex).nextWithTimer();
        if (tsBlock == null) {
          return null;
        } else if (!tsBlock.isEmpty()) {
          LastQueryUtil.appendLastValue(tsBlockBuilder, tsBlock);
          return null;
        }
      } else {
        children.get(currentIndex).close();
        children.set(currentIndex, null);
      }

      currentIndex++;
    }

    TsBlock res = tsBlockBuilder.build();
    tsBlockBuilder.reset();
    return res;
  }

  @Override
  public boolean hasNext() throws Exception {
    return currentIndex < inputOperatorsCount || !tsBlockBuilder.isEmpty();
  }

  @Override
  public boolean isFinished() throws Exception {
    return !hasNextWithTimer();
  }

  @Override
  public void close() throws Exception {
    for (Operator child : children) {
      if (child != null) {
        child.close();
      }
    }
    tsBlockBuilder = null;
  }

  private int getEndIndex() {
    return currentIndex + Math.min(MAX_DETECT_COUNT, inputOperatorsCount - currentIndex);
  }

  @Override
  public long calculateMaxPeekMemory() {
    long maxPeekMemory =
        Math.max(DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES, tsBlockBuilder.getRetainedSizeInBytes());
    long res = 0;
    for (Operator child : children) {
      res = Math.max(res, maxPeekMemory + child.calculateMaxPeekMemory());
    }
    return res;
  }

  @Override
  public long calculateMaxReturnSize() {
    return Math.max(DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES, tsBlockBuilder.getRetainedSizeInBytes());
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    long max = 0;
    for (Operator operator : children) {
      max = Math.max(max, operator.calculateRetainedSizeAfterCallingNext());
    }
    return max;
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + (children == null
            ? 0
            : children.stream()
                .mapToLong(MemoryEstimationHelper::getEstimatedSizeOfAccountableObject)
                .sum())
        + (tsBlockBuilder == null ? 0 : tsBlockBuilder.getRetainedSizeInBytes());
  }
}
