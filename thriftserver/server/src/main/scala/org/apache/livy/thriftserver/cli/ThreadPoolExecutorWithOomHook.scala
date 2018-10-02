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

package org.apache.livy.thriftserver.cli

import java.util.concurrent._

/**
 * This class is taken from Hive, because it is package private so it cannot be accessed.
 * If it will become public we can remove this from here.
 */
class ThreadPoolExecutorWithOomHook(
    corePoolSize: Int,
    maximumPoolSize: Int,
    keepAliveTime: Long,
    unit: TimeUnit,
    workQueue: BlockingQueue[Runnable],
    threadFactory: ThreadFactory,
    val oomHook: Runnable)
  extends ThreadPoolExecutor(
    corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory) {

  override protected def afterExecute(r: Runnable, t: Throwable): Unit = {
    super.afterExecute(r, t)
    if (t == null && r.isInstanceOf[Future[_]] ) {
      try {
        val future: Future[_] = r.asInstanceOf[Future[_]]
        if (future.isDone) {
          future.get
        }
      } catch {
        case _: InterruptedException => Thread.currentThread.interrupt()
        case _: OutOfMemoryError => oomHook.run()
        case _: Throwable => // Do nothing
      }
    } else if (t.isInstanceOf[OutOfMemoryError]) {
      oomHook.run()
    }
  }
}
