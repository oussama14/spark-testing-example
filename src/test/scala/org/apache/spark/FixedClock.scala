package org.apache.spark

import java.util.Date

import scala.concurrent.duration.Duration

class FixedClock(var currentTime: Long) extends org.apache.spark.util.Clock {

  def this() = this(0L)

  def setCurrentTime(time: Date): Unit = synchronized {
    currentTime = time.getTime
    notifyAll()
  }

  def addTime(duration: Duration): Unit = synchronized{
    currentTime += duration.toMillis
    notifyAll()
  }

  override def getTimeMillis(): Long = synchronized {
    currentTime
  }

  override def waitTillTime(targetTime: Long): Long = synchronized {
    while (currentTime < targetTime) {
      wait(10)
    }
    getTimeMillis()
  }
}
