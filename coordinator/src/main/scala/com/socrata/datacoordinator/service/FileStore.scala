package com.socrata.datacoordinator.service

import java.io._

import com.rojoma.simplearm.util._
import java.security.MessageDigest

class FileStore(fileArchive: File) {
  def hexDigit(b: Int): Char = {
    if(b < 10) ('0' + b).toChar
    else ('a' + b - 10).toChar
  }

  def hexString(bs: Array[Byte]) = {
    val sb = new StringBuilder
    var i = 0
    while(i != bs.length) {
      sb.append(hexDigit((bs(i) >> 4) & 0x0f)).append(hexDigit(bs(i) & 0x0f))
      i += 1
    }
    sb.toString
  }

  def store(in: InputStream): String = {
    fileArchive.mkdirs()
    val f = File.createTempFile("tmp","tmp", fileArchive)
    try {
      val digest = MessageDigest.getInstance("SHA-1")
      using(new FileOutputStream(f)) { fa =>
        val buf = new Array[Byte](4096)
        def loop() {
          in.read(buf) match {
            case -1 => // done
            case n => digest.update(buf, 0, n); fa.write(buf, 0, n); loop()
          }
        }
        loop()
      }
      val sha = hexString(digest.digest())
      val truth = new File(fileArchive, sha)
      if(!truth.setLastModified(System.currentTimeMillis())) {
        if(!f.renameTo(truth)) throw new IOException("Unable to rename " + f.getAbsolutePath + " to " + truth.getAbsolutePath)
      }
      sha
    } finally {
      f.delete()
    }
  }

  def open(name: String): InputStream = {
    if(!name.matches("\\p{XDigit}+")) throw new FileNotFoundException
    val file = new File(fileArchive, name)
    new FileInputStream(file)
  }
}
