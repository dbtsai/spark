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

package org.apache.spark.sql.connect.service

import java.io.IOException
import java.net.{InetAddress, InetSocketAddress, ServerSocket}
import java.util.concurrent.{Callable, CountDownLatch, ExecutorService, Executors, Future, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import scala.collection.mutable.ArrayBuffer
import scala.util.control.ControlThrowable

import io.grpc.Server
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}

import org.apache.spark.{SparkConf, SparkContext, SparkException, SparkFunSuite, SparkIllegalStateException}
import org.apache.spark.sql.connect.service.SparkConnectServerLifecycle.{Lifecycle, ServerHandle}
import org.apache.spark.util.Utils

class SparkConnectServerLifecycleSuite
    extends SparkFunSuite
    with BeforeAndAfterEach
    with Eventually {
  private var context: SparkContext = _

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    NativeConnectServiceFixture.reset()
  }

  override protected def afterEach(): Unit = {
    try {
      if (context != null) {
        Utils.tryWithSafeFinally {
          SparkConnectServerLifecycle.stop()
        } {
          context.stop()
        }
      }
    } finally {
      context = null
      super.afterEach()
    }
  }

  private class RecordingHandle extends ServerHandle {
    var waits = 0
    var closes = 0

    override val bindingAddress: InetSocketAddress = new InetSocketAddress("127.0.0.1", 12345)
    override def awaitTermination(): Unit = { waits += 1 }
    override def close(): Unit = { closes += 1 }
  }

  private class TrackingServer(underlying: Server) extends Server {
    var gracefulStops = 0
    var forcedStops = 0
    var timedWait: Option[(Long, TimeUnit)] = None

    override def start(): Server = {
      underlying.start()
      this
    }

    override def shutdown(): Server = {
      gracefulStops += 1
      underlying.shutdown()
      this
    }

    override def shutdownNow(): Server = {
      forcedStops += 1
      underlying.shutdownNow()
      this
    }

    override def isShutdown(): Boolean = underlying.isShutdown
    override def isTerminated(): Boolean = underlying.isTerminated
    override def getPort(): Int = underlying.getPort
    override def awaitTermination(timeout: Long, unit: TimeUnit): Boolean = {
      timedWait = Some((timeout, unit))
      underlying.awaitTermination(timeout, unit)
    }
    override def awaitTermination(): Unit = underlying.awaitTermination()
  }

  private class BlockingNativeClassLoader extends ClassLoader(Utils.getContextOrSparkClassLoader) {
    val requests = new AtomicInteger()
    val failure = new ClassNotFoundException(SparkConnectServerLifecycle.NativeServiceClass)

    override protected def loadClass(name: String, resolve: Boolean): Class[_] = {
      if (name == SparkConnectServerLifecycle.NativeServiceClass) {
        requests.incrementAndGet()
        throw failure
      }
      super.loadClass(name, resolve)
    }
  }

  private def sparkConf(nativeEnabled: Boolean, withPlugin: Boolean = true): SparkConf = {
    val conf = new SparkConf(false)
      .setMaster("local[1]")
      .setAppName(getClass.getSimpleName)
      .set("spark.ui.enabled", "false")
      .set("spark.driver.host", "127.0.0.1")
      .set("spark.driver.bindAddress", "127.0.0.1")
      .set("spark.connect.grpc.binding.address", "127.0.0.1")
      .set("spark.connect.grpc.binding.port", "0")
      .set("spark.connect.native.enabled", nativeEnabled.toString)
    if (withPlugin) {
      conf.set("spark.plugins", "org.apache.spark.sql.connect.SparkConnectPlugin")
    }
    conf
  }

  private def createContext(conf: SparkConf): SparkContext = {
    context = new SparkContext(conf)
    context
  }

  private def startFixture(
      sc: SparkContext,
      loadClass: String => Class[_],
      configuredPort: Int): ServerHandle = {
    SparkConnectServerLifecycle.startNative(sc, loadClass, configuredPort, (_, _) => ())
  }

  private def checkStartupFailure(error: SparkException, reason: String): Unit = {
    checkError(
      exception = error,
      condition = "CONNECT_NATIVE_STARTUP_FAILED",
      sqlState = Some("56K00"),
      parameters = Map(
        "configKey" -> "\"spark.connect.native.enabled\"",
        "serviceClass" -> SparkConnectServerLifecycle.NativeServiceClass,
        "reason" -> reason))
    assert(error.getMessage.contains(reason))
    assert(error.getMessage.contains("--jars or --packages"))
  }

  private def withExecutor(f: ExecutorService => Unit): Unit = {
    val executor = Executors.newFixedThreadPool(3)
    try {
      f(executor)
    } finally {
      val _ = executor.shutdownNow()
      assert(executor.awaitTermination(10, TimeUnit.SECONDS))
    }
  }

  private def submit[T](executor: ExecutorService)(body: => T): Future[T] = {
    executor.submit(new Callable[T] {
      override def call(): T = body
    })
  }

  test("disabled native mode does not resolve or start the optional implementation") {
    val javaHandle = new RecordingHandle
    var javaStarts = 0
    val lifecycle = new Lifecycle(
      _ => { javaStarts += 1; javaHandle },
      _ => throw new AssertionError("Native implementation must not be resolved"))
    lifecycle.start(null, nativeEnabled = false)
    lifecycle.start(null, nativeEnabled = false)
    assert(lifecycle.started)
    assert(javaStarts == 1)
    assert(lifecycle.bindingAddress == javaHandle.bindingAddress)
    lifecycle.awaitTermination()
    assert(javaHandle.waits == 1)
    lifecycle.stop()
    lifecycle.stop()
    assert(javaHandle.closes == 1)
    assert(!lifecycle.started)
  }

  test("enabled native mode starts exactly one frontend and never starts Java") {
    val nativeHandle = new RecordingHandle
    var nativeStarts = 0
    val lifecycle = new Lifecycle(
      _ => throw new AssertionError("Java frontend must not start"),
      _ => { nativeStarts += 1; nativeHandle })
    lifecycle.start(null, nativeEnabled = true)
    lifecycle.start(null, nativeEnabled = true)
    assert(nativeStarts == 1)
    assert(lifecycle.bindingAddress == nativeHandle.bindingAddress)
    lifecycle.stop()
    assert(nativeHandle.closes == 1)
  }

  test("the real disabled driver plugin does not resolve the optional service class") {
    val loader = new BlockingNativeClassLoader
    Utils.withContextClassLoader(loader) {
      createContext(sparkConf(nativeEnabled = false))
      assert(SparkConnectService.started)
      assert(!SparkConnectServerLifecycle.bindingAddress.isUnresolved)
      assert(SparkConnectServerLifecycle.bindingAddress.getPort > 0)
      assert(loader.requests.get() == 0)
    }
  }

  test("the real driver plugin reads native mode and fails clearly when the JAR is missing") {
    val loader = new BlockingNativeClassLoader
    Utils.withContextClassLoader(loader) {
      val error = intercept[SparkException] {
        createContext(sparkConf(nativeEnabled = true))
      }
      checkStartupFailure(error,
        s"Native frontend implementation class not found: ${loader.failure.getMessage}")
      assert(error.getCause eq loader.failure)
      assert(loader.requests.get() > 0)
      assert(!SparkConnectService.started)
    }
  }

  test("Java startup failure after binding releases the real listening port") {
    val sc = createContext(sparkConf(nativeEnabled = false, withPlugin = false))
    val failure = new IOException("setup after bind failed")
    var bound: Server = null
    var address: InetSocketAddress = null
    val error = intercept[IOException] {
      SparkConnectService.start(sc, _ => {
        bound = SparkConnectService.server
        address = SparkConnectService.bindingAddress
        throw failure
      })
    }
    assert(error eq failure)
    assert(bound != null && address != null)
    assert(bound.isShutdown)
    assert(bound.isTerminated)
    assert(!SparkConnectService.started)
    assert(SparkConnectService.server == null)
    Utils.tryWithResource(new ServerSocket()) { socket =>
      socket.setReuseAddress(true)
      socket.bind(address)
    }
  }

  test("a missing optional JAR preserves the cause and does not fall back to Java") {
    val failure = new ClassNotFoundException(SparkConnectServerLifecycle.NativeServiceClass)
    val lifecycle = new Lifecycle(
      _ => throw new AssertionError("Java frontend must not start"),
      sc => startFixture(sc, _ => throw failure, configuredPort = 0))
    val error = intercept[SparkException] {
      lifecycle.start(null, nativeEnabled = true)
    }
    checkStartupFailure(error,
      s"Native frontend implementation class not found: ${failure.getMessage}")
    assert(error.getCause eq failure)
    assert(!lifecycle.started)
    lifecycle.stop()
  }

  test("the versioned optional service handle delegates to the returned handle") {
    val handle = startFixture(
      null, _ => classOf[NativeConnectServiceFixture], configuredPort = 12345)
    val fixture = NativeConnectServiceFixture.lastHandle()
    try {
      assert(handle.bindingAddress eq fixture.bindingAddress())
      handle.awaitTermination()
      assert(fixture.waits() == 1)
      assert(fixture.closes() == 0)
    } finally {
      handle.close()
    }
    assert(fixture.closes() == 1)
  }

  test("optional service runtime startup failures retain their reason and cause") {
    val failure = NativeConnectServiceFixture.startFailure()
    val error = intercept[SparkException] {
      startFixture(
        null, _ => classOf[NativeConnectServiceFixture.FailingService], configuredPort = 0)
    }
    checkStartupFailure(error, failure.getMessage)
    assert(error.getCause eq failure)
  }

  private val contractFailures: Seq[(String, Class[_], Int, Int, String)] = Seq(
    ("API version", classOf[NativeConnectServiceFixture.VersionMismatchService], 0, 0,
      "Unsupported native frontend API version 2; expected 1."),
    ("non-static API version", classOf[NativeConnectServiceFixture.NonStaticVersionService], 0, 0,
      "Native frontendApiVersion must be a static int method."),
    ("API version type", classOf[NativeConnectServiceFixture.WrongVersionReturnService], 0, 0,
      "Native frontendApiVersion must be a static int method."),
    ("non-static start", classOf[NativeConnectServiceFixture.NonStaticStartService], 0, 0,
      "Native service start must be static."),
    ("null handle", classOf[NativeConnectServiceFixture.NullHandleService], 1, 0,
      "Native service start must return a non-null AutoCloseable handle."),
    ("non-closeable handle", classOf[NativeConnectServiceFixture.NonCloseableService], 1, 0,
      "Native service start must return a non-null AutoCloseable handle."),
    ("address return type", classOf[NativeConnectServiceFixture.WrongAddressService], 1, 1,
      "Native service bindingAddress must return InetSocketAddress."),
    ("await return type", classOf[NativeConnectServiceFixture.WrongAwaitService], 1, 1,
      "Native service awaitTermination must return void."),
    ("null address", classOf[NativeConnectServiceFixture.NullAddressService], 1, 1,
      "Native service returned a null binding address."),
    ("unresolved address", classOf[NativeConnectServiceFixture.UnresolvedAddressService], 1, 1,
      "Native service returned an unresolved binding address."),
    ("configured port", classOf[NativeConnectServiceFixture.PortMismatchService], 1, 1,
      "Native service bound port 12346, but configured port is 12345."))

  contractFailures.foreach { case (name, serviceClass, expectedStarts, expectedCloses, reason) =>
    test(s"native contract rejects $name and closes any returned resource") {
      val error = intercept[SparkException] {
        startFixture(null, _ => serviceClass, configuredPort = 12345)
      }
      assert(error.getCause.isInstanceOf[IllegalArgumentException])
      assert(error.getCause.getMessage == reason)
      checkStartupFailure(error, reason)
      assert(NativeConnectServiceFixture.starts() == expectedStarts)
      assert(NativeConnectServiceFixture.closedHandles() == expectedCloses)
    }
  }

  test("an unversioned frontend is rejected before startup") {
    val error = intercept[SparkException] {
      startFixture(
        null, _ => classOf[NativeConnectServiceFixture.MissingVersionService], configuredPort = 0)
    }
    assert(error.getCause.isInstanceOf[NoSuchMethodException])
    checkStartupFailure(error, error.getCause.getMessage)
    assert(NativeConnectServiceFixture.starts() == 0)
  }

  test("an incompatible returned handle is closed and cleanup failure is suppressed") {
    val error = intercept[SparkException] {
      startFixture(
        null, _ => classOf[NativeConnectServiceFixture.IncompatibleService], configuredPort = 0)
    }
    assert(error.getCause.isInstanceOf[NoSuchMethodException])
    checkStartupFailure(error, error.getCause.getMessage)
    assert(NativeConnectServiceFixture.incompatibleCloses() == 1)
    assert(error.getSuppressed.length == 1)
    assert(error.getSuppressed.head.getCause eq NativeConnectServiceFixture.cleanupFailure())
  }

  test("waiting for termination does not prevent a concurrent stop") {
    val waiting = new CountDownLatch(1)
    val stopped = new CountDownLatch(1)
    val handle = new RecordingHandle {
      override def awaitTermination(): Unit = {
        waiting.countDown()
        assert(stopped.await(10, TimeUnit.SECONDS))
      }
      override def close(): Unit = stopped.countDown()
    }
    val lifecycle = new Lifecycle(_ => handle, _ => handle)
    lifecycle.start(null, nativeEnabled = false)
    withExecutor { executor =>
      val waiter = submit(executor) { lifecycle.awaitTermination() }
      assert(waiting.await(10, TimeUnit.SECONDS))
      lifecycle.stop()
      waiter.get(10, TimeUnit.SECONDS)
    }
    assert(!lifecycle.started)
  }

  test("a blocking close leaves state queries available and serializes concurrent stops") {
    val entered = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val closes = new AtomicInteger()
    val secondEntered = new CountDownLatch(1)
    val secondThread = new AtomicReference[Thread]()
    val handle = new RecordingHandle {
      override def close(): Unit = {
        closes.incrementAndGet()
        entered.countDown()
        assert(release.await(10, TimeUnit.SECONDS))
      }
    }
    val lifecycle = new Lifecycle(_ => handle, _ => handle)
    lifecycle.start(null, nativeEnabled = true)
    withExecutor { executor =>
      val first = submit(executor) { lifecycle.stop() }
      try {
        assert(entered.await(10, TimeUnit.SECONDS))
        val state = submit(executor) { lifecycle.started }
        assert(state.get(2, TimeUnit.SECONDS))
        val error = intercept[SparkException] { lifecycle.start(null, nativeEnabled = true) }
        assert(error.getCondition == "INTERNAL_ERROR")
        val second = submit(executor) {
          secondThread.set(Thread.currentThread())
          secondEntered.countDown()
          lifecycle.stop()
        }
        assert(secondEntered.await(10, TimeUnit.SECONDS))
        eventually(timeout(Span(5, Seconds))) {
          assert(!second.isDone)
          assert(secondThread.get().getState == Thread.State.WAITING)
          assert(secondThread.get().getStackTrace.exists { frame =>
            frame.getClassName == classOf[Lifecycle].getName && frame.getMethodName == "stop"
          })
        }
        assert(closes.get() == 1)
        release.countDown()
        first.get(10, TimeUnit.SECONDS)
        second.get(10, TimeUnit.SECONDS)
      } finally {
        release.countDown()
      }
    }
    assert(closes.get() == 1)
    assert(!lifecycle.started)
  }

  test("a new start retries a failed close before replacing the frontend") {
    val failure = new IOException("close failed")
    val handle = new RecordingHandle {
      override def close(): Unit = {
        super.close()
        if (closes == 1) throw failure
      }
    }
    val replacement = new RecordingHandle
    var starts = 0
    val lifecycle = new Lifecycle(_ => new RecordingHandle, _ => {
      starts += 1
      if (starts == 1) handle else replacement
    })
    lifecycle.start(null, nativeEnabled = true)
    assert(intercept[IOException] { lifecycle.stop() } eq failure)
    assert(lifecycle.started)
    lifecycle.start(null, nativeEnabled = true)
    assert(handle.closes == 2)
    assert(starts == 2)
    assert(replacement.closes == 0)
    lifecycle.stop()
    assert(replacement.closes == 1)
    assert(!lifecycle.started)
  }

  Seq(true, false).foreach { nativeEnabled =>
    test(s"failed close retry retains its cause for native replacement=$nativeEnabled") {
      val failure = new IOException("initial close failed")
      val retryFailure = new IOException("retry close failed")
      var failClose = true
      val handle = new RecordingHandle {
        override def close(): Unit = {
          super.close()
          if (failClose) throw (if (closes == 1) failure else retryFailure)
        }
      }
      var starts = 0
      val lifecycle = new Lifecycle(_ => new RecordingHandle, _ => { starts += 1; handle })
      lifecycle.start(null, nativeEnabled = true)
      assert(intercept[IOException] { lifecycle.stop() } eq failure)
      val error = intercept[SparkException] { lifecycle.start(null, nativeEnabled) }
      val replacement = if (nativeEnabled) "native" else "JVM"
      checkStartupFailure(error,
        s"previous native frontend shutdown incomplete; cannot start $replacement frontend")
      assert(error.getCause eq failure)
      assert(error.getSuppressed.toSeq == Seq(retryFailure))
      assert(lifecycle.started)
      assert(lifecycle.bindingAddress == handle.bindingAddress)
      assert(starts == 1)
      failClose = false
      lifecycle.stop()
      assert(handle.closes == 3)
      assert(!lifecycle.started)
    }
  }

  test("graceful stop forwards the Java frontend timeout") {
    var requested: Option[(Long, TimeUnit)] = None
    val handle = new RecordingHandle {
      override def close(timeout: Long, unit: TimeUnit): Unit = {
        requested = Some((timeout, unit))
        super.close()
      }
    }
    val lifecycle = new Lifecycle(_ => handle, _ => handle)
    lifecycle.start(null, nativeEnabled = false)
    lifecycle.stop(Some((1L, TimeUnit.MINUTES)))
    assert(requested.contains((1L, TimeUnit.MINUTES)))
  }

  test("an absent frontend reports the existing structured service-state error") {
    val lifecycle = new Lifecycle(_ => new RecordingHandle, _ => new RecordingHandle)
    checkError(
      exception = intercept[SparkIllegalStateException] { lifecycle.bindingAddress },
      condition = "SPARK_CONNECT_ILLEGAL_STATE.SESSION_MANAGEMENT_SERVICE_NOT_STARTED",
      parameters = Map.empty)
    lifecycle.stop()
  }

  test("providers cannot re-enter shutdown during startup") {
    lazy val lifecycle: Lifecycle = new Lifecycle(_ => {
      lifecycle.stop()
      new RecordingHandle
    }, _ => new RecordingHandle)
    val error = intercept[SparkException] {
      lifecycle.start(null, nativeEnabled = false)
    }
    assert(error.getCondition == "INTERNAL_ERROR")
    assert(!lifecycle.started)
  }

  test("a blocking start leaves state queries available and coordinates a concurrent stop") {
    val entered = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val stopRequested = new CountDownLatch(1)
    val handle = new RecordingHandle
    val lifecycle = new Lifecycle(_ => {
      entered.countDown()
      assert(release.await(10, TimeUnit.SECONDS))
      handle
    }, _ => new RecordingHandle)
    withExecutor { executor =>
      val start = submit(executor) { lifecycle.start(null, nativeEnabled = false) }
      try {
        assert(entered.await(10, TimeUnit.SECONDS))
        val state = submit(executor) { lifecycle.started }
        assert(!state.get(2, TimeUnit.SECONDS))
        val error = intercept[SparkException] { lifecycle.start(null, nativeEnabled = false) }
        assert(error.getCondition == "INTERNAL_ERROR")
        val stop = submit(executor) {
          stopRequested.countDown()
          lifecycle.stop()
        }
        assert(stopRequested.await(10, TimeUnit.SECONDS))
        assert(!stop.isDone)
        release.countDown()
        start.get(10, TimeUnit.SECONDS)
        stop.get(10, TimeUnit.SECONDS)
      } finally {
        release.countDown()
      }
    }
    assert(handle.closes == 1)
    assert(!lifecycle.started)
  }

  test("a provider cannot re-enter stop from its close method") {
    var reenter = true
    lazy val handle: RecordingHandle = new RecordingHandle {
      override def close(): Unit = {
        super.close()
        if (reenter) lifecycle.stop()
      }
    }
    lazy val lifecycle: Lifecycle = new Lifecycle(_ => handle, _ => handle)
    lifecycle.start(null, nativeEnabled = true)
    val error = intercept[SparkException] { lifecycle.stop() }
    assert(error.getCondition == "INTERNAL_ERROR")
    assert(lifecycle.started)
    reenter = false
    lifecycle.stop()
    assert(handle.closes == 2)
    assert(!lifecycle.started)
  }

  test("fatal address failure closes the returned handle and remains unwrapped") {
    val failure = NativeConnectServiceFixture.bindingFailure()
    val error = intercept[VirtualMachineError] {
      startFixture(
        null, _ => classOf[NativeConnectServiceFixture.FatalAddressService], configuredPort = 0)
    }
    assert(error eq failure)
    assert(error.getSuppressed.length == 1)
    assert(error.getSuppressed.head.getCause eq NativeConnectServiceFixture.cleanupFailure())
    assert(NativeConnectServiceFixture.closedHandles() == 1)
  }

  test("linkage and initializer errors retain their cause in the structured startup failure") {
    Seq(new UnsatisfiedLinkError("fixture JNI load failed"),
      new ExceptionInInitializerError("fixture initialization failed")).foreach { failure =>
      val error = intercept[SparkException] {
        startFixture(null, _ => throw failure, configuredPort = 0)
      }
      checkStartupFailure(error, failure.getMessage)
      assert(error.getCause eq failure)
    }
  }

  test("a non-public handle runtime class is rejected and closed") {
    val error = intercept[SparkException] {
      startFixture(
        null, _ => classOf[NativeConnectServiceFixture.NonPublicHandleService], configuredPort = 0)
    }
    assert(error.getCause.isInstanceOf[IllegalArgumentException])
    checkStartupFailure(error, "Native service handle runtime class must be public.")
    assert(NativeConnectServiceFixture.closedHandles() == 1)
  }

  test("dedicated server startup failure keeps its cause and cleans the frontend before session") {
    val calls = ArrayBuffer.empty[String]
    val failure = new IOException("startup failed")
    val closeFailure = new IOException("frontend cleanup failed")
    val sessionFailure = new IOException("session cleanup failed")
    val error = intercept[IOException] {
      SparkConnectServer.run(
        start = { calls += "start"; throw failure },
        waitForTermination = { calls += "await" },
        stop = { calls += "close"; throw closeFailure },
        stopSession = { calls += "session"; throw sessionFailure })
    }
    assert(error eq failure)
    assert(calls.toSeq == Seq("start", "close", "session"))
    assert(error.getSuppressed.toSeq == Seq(closeFailure))
    assert(closeFailure.getSuppressed.toSeq == Seq(sessionFailure))
  }

  test("dedicated server awaits termination before cleaning up in order") {
    val calls = ArrayBuffer.empty[String]
    SparkConnectServer.run(
      start = { calls += "start" },
      waitForTermination = { calls += "await" },
      stop = { calls += "close" },
      stopSession = { calls += "session" })
    assert(calls.toSeq == Seq("start", "await", "close", "session"))
  }

  test("failed Java restart leaves prior listener resources untouched") {
    val sc = createContext(sparkConf(nativeEnabled = false, withPlugin = false))
    try {
      SparkConnectService.start(sc)
      val previousListener = SparkConnectService.listener
      val previousExecutionListener = SparkConnectService.executionListener.get
      SparkConnectService.stop(Some(10), Some(TimeUnit.SECONDS))
      assert(sc.listenerBus.listeners.contains(previousListener))
      assert(sc.listenerBus.listeners.contains(previousExecutionListener))
      Utils.tryWithResource(
        new ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"))) { socket =>
        withSparkEnvConfs(
          "spark.connect.grpc.binding.port" -> socket.getLocalPort.toString,
          "spark.connect.grpc.port.maxRetries" -> "0") {
          val error = intercept[IOException] { SparkConnectService.start(sc) }
          assert(Utils.isBindCollision(error))
        }
      }
      assert(sc.listenerBus.listeners.contains(previousListener))
      assert(sc.listenerBus.listeners.contains(previousExecutionListener))
      assert(SparkConnectService.uiTab.isEmpty)
      assert(SparkConnectService.listener == null)
      assert(SparkConnectService.executionListener.isEmpty)
      assert(SparkConnectService.server == null)
      assert(!SparkConnectService.started)
    } finally {
      if (SparkConnectService.started) SparkConnectService.stop()
    }
  }

  test("failed contract cleanup retains the same raw handle for the next start") {
    val sc = createContext(sparkConf(nativeEnabled = true, withPlugin = false))
    val lifecycle = SparkConnectServerLifecycle.createLifecycle(_ => {
      if (NativeConnectServiceFixture.starts() == 0) {
        classOf[NativeConnectServiceFixture.RetryCleanupService]
      } else {
        classOf[NativeConnectServiceFixture]
      }
    })
    val error = intercept[SparkException] { lifecycle.start(sc, nativeEnabled = true) }
    val rawHandle = NativeConnectServiceFixture.lastRetryCleanupHandle()
    val pending = error.getSuppressed.head.asInstanceOf[AutoCloseable]
    assert(rawHandle.closes() == 1)
    assert(lifecycle.started)
    intercept[SparkIllegalStateException] { lifecycle.bindingAddress }
    intercept[SparkIllegalStateException] { lifecycle.awaitTermination() }
    lifecycle.start(sc, nativeEnabled = true)
    assert(rawHandle.closes() == 2)
    assert(NativeConnectServiceFixture.starts() == 2)
    lifecycle.stop()
    pending.close()
    assert(rawHandle.closes() == 2)
    assert(!lifecycle.started)
  }

  test("fatal contract failure retains cleanup ownership without wrapping the failure") {
    val sc = createContext(sparkConf(nativeEnabled = true, withPlugin = false))
    val lifecycle = SparkConnectServerLifecycle.createLifecycle(
      _ => classOf[NativeConnectServiceFixture.FatalAddressService])
    val failure = NativeConnectServiceFixture.bindingFailure()
    assert(intercept[VirtualMachineError] {
      lifecycle.start(sc, nativeEnabled = true)
    } eq failure)
    assert(lifecycle.started)
    assert(NativeConnectServiceFixture.closedHandles() == 1)
    lifecycle.stop()
    assert(NativeConnectServiceFixture.closedHandles() == 2)
    failure.getSuppressed.head.asInstanceOf[AutoCloseable].close()
    assert(NativeConnectServiceFixture.closedHandles() == 2)
    assert(!lifecycle.started)
  }

  test("a reused fatal startup failure cannot replace the current cleanup owner with an old one") {
    val sc = createContext(sparkConf(nativeEnabled = true, withPlugin = false))
    val lifecycle = SparkConnectServerLifecycle.createLifecycle(
      _ => classOf[NativeConnectServiceFixture.FatalAddressService])
    val failure = NativeConnectServiceFixture.bindingFailure()
    assert(intercept[VirtualMachineError] {
      lifecycle.start(sc, nativeEnabled = true)
    } eq failure)
    val firstHandle = NativeConnectServiceFixture.lastFatalAddressHandle()
    assert(failure.getSuppressed.length == 1)
    assert(firstHandle.closes() == 1)
    assert(intercept[VirtualMachineError] {
      lifecycle.start(sc, nativeEnabled = true)
    } eq failure)
    val secondHandle = NativeConnectServiceFixture.lastFatalAddressHandle()
    assert(failure.getSuppressed.length == 2)
    assert(secondHandle ne firstHandle)
    assert(firstHandle.closes() == 2)
    assert(secondHandle.closes() == 1)
    assert(lifecycle.started)
    lifecycle.stop()
    assert(firstHandle.closes() == 2)
    assert(secondHandle.closes() == 2)
    assert(!lifecycle.started)
  }

  test("the production native lifecycle factory validates the SparkContext configured port") {
    val sc = createContext(sparkConf(nativeEnabled = true, withPlugin = false)
      .set("spark.connect.grpc.binding.port", "12346"))
    val lifecycle = SparkConnectServerLifecycle.createLifecycle(
      _ => classOf[NativeConnectServiceFixture])
    val error = intercept[SparkException] { lifecycle.start(sc, nativeEnabled = true) }
    checkStartupFailure(error,
      "Native service bound port 12345, but configured port is 12346.")
    assert(NativeConnectServiceFixture.starts() == 1)
    assert(NativeConnectServiceFixture.closedHandles() == 1)
    assert(!lifecycle.started)
  }

  test("the production Java lifecycle forwards graceful timeout to the real service") {
    val sc = createContext(sparkConf(nativeEnabled = false, withPlugin = false))
    val lifecycle = SparkConnectServerLifecycle.createLifecycle(_ =>
      throw new AssertionError("Native implementation must not be resolved"))
    lifecycle.start(sc, nativeEnabled = false)
    val actualServer = SparkConnectService.server
    val trackedServer = new TrackingServer(actualServer)
    SparkConnectService.server = trackedServer
    try {
      lifecycle.stop(Some((10L, TimeUnit.SECONDS)))
      assert(trackedServer.gracefulStops == 1)
      assert(trackedServer.forcedStops == 0)
      assert(trackedServer.timedWait.contains((10L, TimeUnit.SECONDS)))
      assert(actualServer.isTerminated)
      assert(!SparkConnectService.started)
      assert(!lifecycle.started)
    } finally {
      try {
        lifecycle.stop()
      } finally {
        SparkConnectService.server = actualServer
        actualServer.shutdownNow()
        assert(actualServer.awaitTermination(10, TimeUnit.SECONDS))
      }
    }
  }

  test("production startup retains cleanup ownership when the failure disables suppression") {
    val sc = createContext(sparkConf(nativeEnabled = true, withPlugin = false))
    val failure = new ControlThrowable {}
    NativeConnectServiceFixture.setBindingAction(new Runnable {
      override def run(): Unit = throw failure
    })
    val lifecycle = SparkConnectServerLifecycle.createLifecycle(
      _ => classOf[NativeConnectServiceFixture.FatalAddressService])
    assert(intercept[ControlThrowable] { lifecycle.start(sc, nativeEnabled = true) } eq failure)
    assert(failure.getSuppressed.isEmpty)
    assert(lifecycle.started)
    assert(NativeConnectServiceFixture.closedHandles() == 1)
    lifecycle.stop()
    assert(NativeConnectServiceFixture.closedHandles() == 2)
    assert(!lifecycle.started)
  }

  test("interrupted native startup cleanup restores the thread flag and retains ownership") {
    val wasInterrupted = Thread.interrupted()
    var lifecycle: Lifecycle = null
    try {
      val sc = createContext(sparkConf(nativeEnabled = true, withPlugin = false))
      lifecycle = SparkConnectServerLifecycle.createLifecycle(
        _ => classOf[NativeConnectServiceFixture.InterruptedCleanupService])
      val error = intercept[SparkException] { lifecycle.start(sc, nativeEnabled = true) }
      assert(error.getCause.isInstanceOf[NoSuchMethodException])
      assert(error.getSuppressed.head.getCause eq NativeConnectServiceFixture.cleanupInterruption())
      assert(Thread.interrupted())
      assert(lifecycle.started)
      assert(NativeConnectServiceFixture.closedHandles() == 1)
      lifecycle.stop()
      assert(NativeConnectServiceFixture.closedHandles() == 2)
      assert(!lifecycle.started)
    } finally {
      val _ = Thread.interrupted()
      Utils.tryWithSafeFinally {
        if (lifecycle != null) lifecycle.stop()
      } {
        if (wasInterrupted) Thread.currentThread().interrupt()
      }
    }
  }

}
