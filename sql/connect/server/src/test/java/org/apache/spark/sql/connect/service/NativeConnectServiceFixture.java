/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.connect.service;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.spark.SparkContext;

/**
 * Public reflective fixtures; these do not start Spark or load a native library.
 */
public final class NativeConnectServiceFixture {
  private static final AtomicInteger STARTS = new AtomicInteger();
  private static final AtomicInteger CLOSED_HANDLES = new AtomicInteger();
  private static final AtomicInteger INCOMPATIBLE_CLOSES = new AtomicInteger();

  private static volatile IllegalStateException startFailure = newStartFailure();
  private static volatile IOException cleanupFailure = newCleanupFailure();
  private static volatile VirtualMachineError bindingFailure = newBindingFailure();
  private static volatile Runnable bindingAction;
  private static volatile InterruptedException cleanupInterruption = newCleanupInterruption();
  private static volatile Handle lastHandle;
  private static volatile RetryCleanupHandle lastRetryCleanupHandle;
  private static volatile FatalAddressHandle lastFatalAddressHandle;

  private NativeConnectServiceFixture() {
  }

  private static IllegalStateException newStartFailure() {
    return new IllegalStateException("fixture startup failed");
  }

  private static IOException newCleanupFailure() {
    return new IOException("fixture cleanup failed");
  }

  private static InterruptedException newCleanupInterruption() {
    return new InterruptedException("fixture cleanup interrupted");
  }

  private static VirtualMachineError newBindingFailure() {
    return new OutOfMemoryError("fixture binding failed");
  }

  public static void reset() {
    lastHandle = null;
    lastRetryCleanupHandle = null;
    lastFatalAddressHandle = null;
    startFailure = newStartFailure();
    cleanupFailure = newCleanupFailure();
    bindingFailure = newBindingFailure();
    bindingAction = null;
    cleanupInterruption = newCleanupInterruption();
    STARTS.set(0);
    CLOSED_HANDLES.set(0);
    INCOMPATIBLE_CLOSES.set(0);
  }

  public static int frontendApiVersion() {
    return 1;
  }

  public static AutoCloseable start(SparkContext sc) {
    STARTS.incrementAndGet();
    lastHandle = new Handle();
    return lastHandle;
  }

  public static Handle lastHandle() {
    return lastHandle;
  }

  public static RetryCleanupHandle lastRetryCleanupHandle() {
    return lastRetryCleanupHandle;
  }

  public static IllegalStateException startFailure() {
    return startFailure;
  }

  public static IOException cleanupFailure() {
    return cleanupFailure;
  }

  public static FatalAddressHandle lastFatalAddressHandle() {
    return lastFatalAddressHandle;
  }

  public static void setBindingAction(Runnable action) {
    bindingAction = action;
  }

  public static InterruptedException cleanupInterruption() {
    return cleanupInterruption;
  }

  public static VirtualMachineError bindingFailure() {
    return bindingFailure;
  }

  public static int starts() {
    return STARTS.get();
  }

  public static int closedHandles() {
    return CLOSED_HANDLES.get();
  }

  public static int incompatibleCloses() {
    return INCOMPATIBLE_CLOSES.get();
  }

  public static final class Handle implements AutoCloseable {
    private final InetSocketAddress address;
    private final AtomicInteger waits = new AtomicInteger();
    private final AtomicInteger closes = new AtomicInteger();

    private Handle() {
      this(new InetSocketAddress("127.0.0.1", 12345));
    }

    private Handle(InetSocketAddress address) {
      this.address = address;
    }

    public InetSocketAddress bindingAddress() {
      return address;
    }

    public void awaitTermination() {
      waits.incrementAndGet();
    }

    @Override
    public void close() {
      closes.incrementAndGet();
      CLOSED_HANDLES.incrementAndGet();
    }

    public int waits() {
      return waits.get();
    }

    public int closes() {
      return closes.get();
    }
  }

  public static class BaseService {
    public static int frontendApiVersion() {
      return 1;
    }
  }

  public static final class NonStaticVersionService {
    public int frontendApiVersion() {
      return 1;
    }

    public static AutoCloseable start(SparkContext sc) {
      STARTS.incrementAndGet();
      return new Handle();
    }
  }

  public static final class WrongVersionReturnService {
    public static String frontendApiVersion() {
      return "1";
    }

    public static AutoCloseable start(SparkContext sc) {
      STARTS.incrementAndGet();
      return new Handle();
    }
  }

  public static final class MissingVersionService {
    public static AutoCloseable start(SparkContext sc) {
      STARTS.incrementAndGet();
      return new Handle();
    }
  }

  public static final class VersionMismatchService extends BaseService {
    public static int frontendApiVersion() {
      return 2;
    }

    public static AutoCloseable start(SparkContext sc) {
      STARTS.incrementAndGet();
      return new Handle();
    }
  }

  public static final class NonStaticStartService extends BaseService {
    public AutoCloseable start(SparkContext sc) {
      STARTS.incrementAndGet();
      return new Handle();
    }
  }

  public static final class NullHandleService extends BaseService {
    public static AutoCloseable start(SparkContext sc) {
      STARTS.incrementAndGet();
      return null;
    }
  }

  public static final class NonCloseableService extends BaseService {
    public static Object start(SparkContext sc) {
      STARTS.incrementAndGet();
      return new Object();
    }
  }

  public static final class WrongAddressService extends BaseService {
    public static AutoCloseable start(SparkContext sc) {
      STARTS.incrementAndGet();
      return new WrongAddressHandle();
    }
  }

  public static final class WrongAddressHandle implements AutoCloseable {
    public String bindingAddress() {
      return "127.0.0.1:12345";
    }

    public void awaitTermination() {
    }

    @Override
    public void close() {
      CLOSED_HANDLES.incrementAndGet();
    }
  }

  public static final class WrongAwaitService extends BaseService {
    public static AutoCloseable start(SparkContext sc) {
      STARTS.incrementAndGet();
      return new WrongAwaitHandle();
    }
  }

  public static final class WrongAwaitHandle implements AutoCloseable {
    public InetSocketAddress bindingAddress() {
      return new InetSocketAddress("127.0.0.1", 12345);
    }

    public int awaitTermination() {
      return 0;
    }

    @Override
    public void close() {
      CLOSED_HANDLES.incrementAndGet();
    }
  }

  public static final class NullAddressService extends BaseService {
    public static AutoCloseable start(SparkContext sc) {
      STARTS.incrementAndGet();
      return new Handle(null);
    }
  }

  public static final class UnresolvedAddressService extends BaseService {
    public static AutoCloseable start(SparkContext sc) {
      STARTS.incrementAndGet();
      return new Handle(InetSocketAddress.createUnresolved("127.0.0.1", 12345));
    }
  }

  public static final class PortMismatchService extends BaseService {
    public static AutoCloseable start(SparkContext sc) {
      STARTS.incrementAndGet();
      return new Handle(new InetSocketAddress("127.0.0.1", 12346));
    }
  }

  public static final class FailingService extends BaseService {
    public static AutoCloseable start(SparkContext sc) {
      STARTS.incrementAndGet();
      throw startFailure;
    }
  }

  public static final class FatalAddressService extends BaseService {
    public static AutoCloseable start(SparkContext sc) {
      STARTS.incrementAndGet();
      lastFatalAddressHandle = new FatalAddressHandle();
      return lastFatalAddressHandle;
    }
  }

  public static final class FatalAddressHandle implements AutoCloseable {
    private final AtomicInteger closes = new AtomicInteger();

    public InetSocketAddress bindingAddress() {
      Runnable action = bindingAction;
      if (action != null) {
        action.run();
      }
      throw bindingFailure;
    }

    public void awaitTermination() {
    }

    @Override
    public void close() throws IOException {
      CLOSED_HANDLES.incrementAndGet();
      if (closes.incrementAndGet() == 1) {
        throw cleanupFailure;
      }
    }

    public int closes() {
      return closes.get();
    }
  }

  public static final class NonPublicHandleService extends BaseService {
    public static AutoCloseable start(SparkContext sc) {
      STARTS.incrementAndGet();
      return new NonPublicHandle();
    }
  }

  private static final class NonPublicHandle implements AutoCloseable {
    public InetSocketAddress bindingAddress() {
      return new InetSocketAddress("127.0.0.1", 12345);
    }

    public void awaitTermination() {
    }

    @Override
    public void close() {
      CLOSED_HANDLES.incrementAndGet();
    }
  }

  public static final class RetryCleanupService extends BaseService {
    public static AutoCloseable start(SparkContext sc) {
      STARTS.incrementAndGet();
      lastRetryCleanupHandle = new RetryCleanupHandle();
      return lastRetryCleanupHandle;
    }
  }

  public static final class RetryCleanupHandle implements AutoCloseable {
    private final AtomicInteger closes = new AtomicInteger();

    public InetSocketAddress bindingAddress() {
      return new InetSocketAddress("127.0.0.1", 12345);
    }

    // Intentionally missing awaitTermination().
    @Override
    public void close() throws IOException {
      CLOSED_HANDLES.incrementAndGet();
      if (closes.incrementAndGet() == 1) {
        throw cleanupFailure;
      }
    }

    public int closes() {
      return closes.get();
    }
  }

  public static final class InterruptedCleanupService extends BaseService {
    public static AutoCloseable start(SparkContext sc) {
      STARTS.incrementAndGet();
      return new InterruptedCleanupHandle();
    }
  }

  public static final class InterruptedCleanupHandle implements AutoCloseable {
    private final AtomicInteger closes = new AtomicInteger();

    public InetSocketAddress bindingAddress() {
      return new InetSocketAddress("127.0.0.1", 12345);
    }

    // Intentionally missing awaitTermination().
    @Override
    public void close() throws InterruptedException {
      CLOSED_HANDLES.incrementAndGet();
      if (closes.incrementAndGet() == 1) {
        throw cleanupInterruption;
      }
    }
  }

  public static final class IncompatibleService extends BaseService {
    public static AutoCloseable start(SparkContext sc) {
      STARTS.incrementAndGet();
      return new IncompatibleHandle();
    }
  }

  public static final class IncompatibleHandle implements AutoCloseable {
    public InetSocketAddress bindingAddress() {
      return new InetSocketAddress("127.0.0.1", 12345);
    }

    // Intentionally missing awaitTermination().
    @Override
    public void close() throws IOException {
      CLOSED_HANDLES.incrementAndGet();
      INCOMPATIBLE_CLOSES.incrementAndGet();
      throw cleanupFailure;
    }
  }
}
