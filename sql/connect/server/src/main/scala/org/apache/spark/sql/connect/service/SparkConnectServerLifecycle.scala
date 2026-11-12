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

import java.lang.reflect.{InvocationTargetException, Method, Modifier}
import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

import scala.util.control.NonFatal

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.sql.catalyst.util.QuotingUtils.toSQLConf
import org.apache.spark.sql.connect.IllegalStateErrors
import org.apache.spark.sql.connect.config.Connect.{CONNECT_GRPC_BINDING_PORT, CONNECT_NATIVE_ENABLED}
import org.apache.spark.util.Utils

/** Selects one Spark Connect frontend without linking the optional native implementation. */
private[connect] object SparkConnectServerLifecycle {
  private[service] val NativeServiceClass =
    "org.apache.spark.sql.connect.nativebridge.engine.NativeConnectService"
  private[service] val FrontendApiVersion = 1

  private[service] trait ServerHandle extends AutoCloseable {
    def bindingAddress: InetSocketAddress
    def awaitTermination(): Unit
    // Native providers apply their own drain/retry policy; failed close retains ownership.
    def close(timeout: Long, unit: TimeUnit): Unit = close()
  }

  private[service] final class Lifecycle(
      startJava: SparkContext => ServerHandle,
      startNative: SparkContext => ServerHandle) {
    private var server: Option[ServerHandle] = None
    private var nativeFrontend = false
    private var starting = false
    private var startingThread: Thread = _
    private var stopping = false
    private var closeFailure: Option[Throwable] = None
    private var closingThread: Thread = _

    // Providers borrow SparkContext and must not re-enter this lifecycle.
    def start(sc: SparkContext, nativeEnabled: Boolean): Unit = {
      val previous = synchronized {
        if (starting || stopping) {
          throw SparkException.internalError(
            "Cannot start a Spark Connect frontend during startup or shutdown.")
        }
        if (server.isDefined && closeFailure.isEmpty) return
        starting = true
        startingThread = Thread.currentThread()
        server.flatMap(handle => closeFailure.map(failure => (handle, nativeFrontend, failure)))
      }
      try {
        previous.foreach { case (handle, wasNative, failure) =>
          try {
            closeAndUpdate(handle, None)
          } catch {
            case e if wasNative && (NonFatal(e) || e.isInstanceOf[LinkageError]) =>
              val replacement = if (nativeEnabled) "native" else "JVM"
              val error = nativeStartupFailure(failure,
                s"previous native frontend shutdown incomplete; cannot start $replacement frontend")
              if (e ne failure) error.addSuppressed(e)
              throw error
          }
        }
        val handle = if (nativeEnabled) startNative(sc) else startJava(sc)
        synchronized {
          server = Some(handle)
          nativeFrontend = nativeEnabled
        }
      } finally {
        synchronized {
          starting = false
          startingThread = null
          notifyAll()
        }
      }
    }

    private[service] def retainFailedCleanup(handle: ServerHandle, failure: Throwable): Unit = {
      synchronized {
        server = Some(handle)
        nativeFrontend = true
        closeFailure = Some(failure)
      }
    }

    def started: Boolean = synchronized { server.isDefined }

    private def current: ServerHandle = synchronized {
      server.getOrElse(throw IllegalStateErrors.serviceNotStarted())
    }

    def bindingAddress: InetSocketAddress = current.bindingAddress

    def awaitTermination(): Unit = current.awaitTermination()

    // A failed close retains ownership until a subsequent stop or startup retry succeeds.
    def stop(timeout: Option[(Long, TimeUnit)] = None): Unit = {
      val selected = synchronized {
        if ((startingThread eq Thread.currentThread()) ||
            (closingThread eq Thread.currentThread())) {
          throw SparkException.internalError(
            "Spark Connect frontend providers must not re-enter the server lifecycle.")
        }
        while (starting || stopping) {
          wait()
        }
        server.foreach { _ =>
          stopping = true
          closingThread = Thread.currentThread()
        }
        server
      }
      selected.foreach { handle =>
        try {
          closeAndUpdate(handle, timeout)
        } finally {
          synchronized {
            stopping = false
            closingThread = null
            notifyAll()
          }
        }
      }
    }

    private def closeAndUpdate(
        handle: ServerHandle,
        timeout: Option[(Long, TimeUnit)]): Unit = {
      var failure: Throwable = null
      try {
        timeout match {
          case Some((duration, unit)) => handle.close(duration, unit)
          case None => handle.close()
        }
      } catch {
        case e: Throwable =>
          failure = e
          throw e
      } finally {
        synchronized {
          if (failure == null) {
            server = None
            closeFailure = None
            nativeFrontend = false
          } else {
            closeFailure = closeFailure.orElse(Some(failure))
          }
        }
      }
    }
  }

  private[service] def createLifecycle(loadClass: String => Class[_]): Lifecycle = {
    lazy val selected: Lifecycle = new Lifecycle(startJava, sc => startNative(
      sc, loadClass, sc.conf.get(CONNECT_GRPC_BINDING_PORT),
      (handle, failure) => selected.retainFailedCleanup(handle, failure)))
    selected
  }

  private val lifecycle = createLifecycle(name => Utils.classForName[AnyRef](name))

  def start(sc: SparkContext): Unit = {
    lifecycle.start(sc, sc.conf.get(CONNECT_NATIVE_ENABLED))
  }

  def bindingAddress: InetSocketAddress = lifecycle.bindingAddress

  def awaitTermination(): Unit = lifecycle.awaitTermination()

  def stop(): Unit = lifecycle.stop()

  def stop(timeout: Long, unit: TimeUnit): Unit = lifecycle.stop(Some((timeout, unit)))

  private def startJava(sc: SparkContext): ServerHandle = {
    SparkConnectService.start(sc)
    new ServerHandle {
      override def bindingAddress: InetSocketAddress = SparkConnectService.bindingAddress
      override def awaitTermination(): Unit = SparkConnectService.server.awaitTermination()
      override def close(): Unit = SparkConnectService.stop()
      override def close(timeout: Long, unit: TimeUnit): Unit = {
        SparkConnectService.stop(Some(timeout), Some(unit))
      }
    }
  }

  /**
   * Internal reflective contract, not a stable developer API. The optional Apache frontend JAR
   * must export frontendApiVersion(): int and start(SparkContext): AutoCloseable as static methods.
   * It returns a fresh, exclusively caller-owned handle whose runtime class is public and supplies
   * bindingAddress(): InetSocketAddress and awaitTermination(): void. A second active start fails.
   * The current implementation requires an application-owned default classic SparkSession with
   * this SparkContext before the first client request. It creates private sessions using the
   * default session's shared state, context and extensions. Runtime SQL confs and session-local
   * temp views are not inherited; global temp views and cache remain shared. It must not stop the
   * application-owned session or context, or call back into this lifecycle. It must honor the
   * configured port and fail closed on unsupported bind/authentication settings. Native close
   * applies the provider's bounded drain/retry policy and may fail; Spark retains ownership for
   * a later stop/start retry.
   * awaitTermination returns after native drain completes; Java-side cleanup can still fail and
   * close remains retryable. Listener exit alone must not complete the native-drain wait.
   * Keep all implementation class resolution inside this enabled-only path.
   */
  private[service] def startNative(
      sc: SparkContext,
      loadClass: String => Class[_],
      configuredPort: Int,
      retainFailedCleanup: (ServerHandle, Throwable) => Unit): ServerHandle = {
    var resource: AutoCloseable = null
    try {
      val serviceClass = loadClass(NativeServiceClass)
      val apiVersion = serviceClass.getMethod("frontendApiVersion")
      check(Modifier.isStatic(apiVersion.getModifiers) &&
        apiVersion.getReturnType == java.lang.Integer.TYPE,
        "Native frontendApiVersion must be a static int method.")
      val version = invoke(apiVersion, null).asInstanceOf[java.lang.Integer].intValue()
      check(version == FrontendApiVersion,
        s"Unsupported native frontend API version $version; expected $FrontendApiVersion.")
      val start = serviceClass.getMethod("start", classOf[SparkContext])
      check(Modifier.isStatic(start.getModifiers), "Native service start must be static.")
      resource = invoke(start, null, sc) match {
        case handle: AutoCloseable => handle
        case _ => throw new IllegalArgumentException(
          "Native service start must return a non-null AutoCloseable handle.")
      }
      check(Modifier.isPublic(resource.getClass.getModifiers),
        "Native service handle runtime class must be public.")
      val addressMethod = resource.getClass.getMethod("bindingAddress")
      val awaitMethod = resource.getClass.getMethod("awaitTermination")
      check(addressMethod.getReturnType == classOf[InetSocketAddress],
        "Native service bindingAddress must return InetSocketAddress.")
      check(awaitMethod.getReturnType == java.lang.Void.TYPE,
        "Native service awaitTermination must return void.")
      val address = invoke(addressMethod, resource).asInstanceOf[InetSocketAddress]
      check(address != null, "Native service returned a null binding address.")
      check(!address.isUnresolved, "Native service returned an unresolved binding address.")
      check(configuredPort == 0 || address.getPort == configuredPort,
        s"Native service bound port ${address.getPort}, but configured port is $configuredPort.")
      val handle = resource
      new ServerHandle {
        override def bindingAddress: InetSocketAddress = address
        override def awaitTermination(): Unit = {
          invoke(awaitMethod, handle)
        }
        override def close(): Unit = handle.close()
      }
    } catch {
      case e if NonFatal(e) || e.isInstanceOf[LinkageError] =>
        val reason = e match {
          case missing: ClassNotFoundException =>
            val name = Option(missing.getMessage).getOrElse(NativeServiceClass)
            s"Native frontend implementation class not found: $name"
          case _ => Option(e.getMessage).getOrElse(e.getClass.getName)
        }
        failNativeStart(resource, nativeStartupFailure(e, reason), retainFailedCleanup)
      case e: Throwable => failNativeStart(resource, e, retainFailedCleanup)
    }
  }

  /** Retains an unvalidated provider only for cleanup; it is never a serving frontend. */
  private final class PendingNativeCleanup(
      initialResource: AutoCloseable,
      closeFailure: Throwable,
      startupFailure: Throwable)
      extends Exception(
        "Native frontend startup cleanup incomplete; retry close.",
        if (closeFailure eq startupFailure) null else closeFailure)
      with ServerHandle {
    @transient private var resource = initialResource

    override def bindingAddress: InetSocketAddress = throw IllegalStateErrors.serviceNotStarted()
    override def awaitTermination(): Unit = throw IllegalStateErrors.serviceNotStarted()
    override def close(): Unit = synchronized {
      if (resource != null) {
        resource.close()
        // The thrown startup error can outlive this lifecycle; do not pin a closed provider.
        resource = null
      }
    }
  }

  private def failNativeStart(
      resource: AutoCloseable,
      failure: Throwable,
      retainFailedCleanup: (ServerHandle, Throwable) => Unit): Nothing = {
    if (resource != null) {
      try {
        resource.close()
      } catch {
        case cleanupFailure: Throwable =>
          if (cleanupFailure.isInstanceOf[InterruptedException]) {
            Thread.currentThread().interrupt()
          }
          val pending = new PendingNativeCleanup(resource, cleanupFailure, failure)
          // Some fatal/control throwables disable suppression, so ownership cannot rely on it.
          retainFailedCleanup(pending, cleanupFailure)
          failure.addSuppressed(pending)
      }
    }
    throw failure
  }

  private def check(condition: Boolean, message: => String): Unit = {
    if (!condition) throw new IllegalArgumentException(message)
  }

  private def nativeStartupFailure(cause: Throwable, reason: String): SparkException = {
    new SparkException(
      errorClass = "CONNECT_NATIVE_STARTUP_FAILED",
      messageParameters = Map(
        "configKey" -> toSQLConf(CONNECT_NATIVE_ENABLED.key),
        "serviceClass" -> NativeServiceClass,
        "reason" -> reason),
      cause = cause)
  }

  private def invoke(method: Method, target: AnyRef, args: AnyRef*): AnyRef = {
    try {
      method.invoke(target, args: _*)
    } catch {
      case e: InvocationTargetException if e.getCause != null => throw e.getCause
    }
  }
}
