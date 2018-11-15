package org.radekbor

import java.io.{DataInput, DataOutput}

import org.apache.hadoop.io.{Text, Writable}

class StringWritable(val content: String) extends Writable {

  def this() = this("")

  val contentWritable = new Text(content)

  override def write(out: DataOutput): Unit = {
    contentWritable.write(out)
  }

  override def readFields(in: DataInput): Unit = {
    contentWritable.readFields(in)
  }

  override def toString: String = contentWritable.toString
}
