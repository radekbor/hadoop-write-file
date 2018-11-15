package org.radekbor

import java.util
import java.util.logging.Logger

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, MapFile}
import org.apache.hadoop.io.SequenceFile.Reader
import org.apache.hadoop.io.SequenceFile.Writer

import scala.collection.mutable.ListBuffer

case class WriteSequenceFile(fileName: String) {


  def write(): Unit = {

    import org.apache.hadoop.fs.FileSystem

    val uri = fileName
    import org.apache.hadoop.io.SequenceFile

    val conf = new org.apache.hadoop.conf.Configuration()

    val fs = FileSystem.get(conf)

    try {
      val writer = SequenceFile.createWriter(conf,
        Writer.file(new Path(uri)),
        Writer.keyClass(classOf[IntWritable]),
        Writer.valueClass(classOf[IntWritable]),
        Writer.compression(SequenceFile.CompressionType.NONE))
      for (x <- 1 to 100) {
        writer.append(new IntWritable(x), new IntWritable(x * x))
      }
      writer.sync()
      writer.close()
    }
  }

}

case class WriteMapFile(fileName: String) {


  def write(): Unit = {

    import org.apache.hadoop.fs.FileSystem

    val uri = fileName
    import org.apache.hadoop.io.MapFile.Writer

    val conf = new org.apache.hadoop.conf.Configuration()

    val fs = FileSystem.get(conf)

    try {
      val writer = new MapFile.Writer(conf,
        new Path(uri),
        Writer.keyClass(classOf[IntWritable]),
        Writer.valueClass(classOf[IntWritable]))
//        Writer.compression(SequenceFile.CompressionType.NONE))
      for (x <- 1 to 100) {
        writer.append(new IntWritable(x), new IntWritable(x * x))
      }
      writer.close()
    }
  }

}

case class ReadSequenceFile(fileName: String) {

  private val log = Logger.getLogger(classOf[ReadSequenceFile].getName)

  def read() = {

    import org.apache.hadoop.fs.FileSystem

    val uri = fileName
    import org.apache.hadoop.io.SequenceFile

    val conf = new org.apache.hadoop.conf.Configuration()

    val fs = FileSystem.get(conf)

    try {
      val reader = new SequenceFile.Reader(conf,
        Reader.file(new Path(uri)))

      val key = new IntWritable
      val value = new IntWritable


      val result = new ListBuffer[String]
      while (reader.next(key, value)) {
        result += "(" + key.get() + " " + value.get() + ")"
      }
      result.toList
    }
  }

}

object ReadAndWrite {

  private val log = Logger.getLogger(WriteSequenceFile.toString)

  def main(arg: Array[String]): Unit = {
    val label: String = util.Arrays.toString(arg.asInstanceOf[Array[Object]])
    log.info(label)

    val sequenceFileName = s"./out/sequence/file.data"
    WriteSequenceFile(sequenceFileName).write()

    val mapFileName = s"./out/map/file.data"
    WriteMapFile(mapFileName).write()

    val content = ReadSequenceFile(sequenceFileName).read()
    log.info(content.mkString(","))
  }

}
