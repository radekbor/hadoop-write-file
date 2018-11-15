package org.radekbor

import java.time.LocalDate
import java.util.logging.Logger

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, MapFile}


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
        Writer.valueClass(classOf[StringWritable]))
      for (x <- 1 to 100) {
        writer.append(new IntWritable(x), new StringWritable(LocalDate.now().plusDays(x).toString))
      }
      writer.close()
    }
  }

}

object ReadAndWriteMap {

  private val log = Logger.getLogger(WriteSequenceFile.toString)

  def main(arg: Array[String]): Unit = {

    val mapFileName = s"./out/map/file.data"
    WriteMapFile(mapFileName).write()

  }

}
