package com.socrata.querycoordinator.util

import java.io.File
import java.util.UUID

import com.rojoma.simplearm.v2.{managed => sManaged, _}
import org.apache.commons.io.FileUtils

object TemporaryDirectory {
  private def tempName(within: Option[File]) =
    new File(within.getOrElse(new File(System.getProperty("java.io.tmpdir"))), UUID.randomUUID().toString)

  private object DirectoryResource extends Resource[File] {
    override def close(a: File): Unit = FileUtils.deleteDirectory(a)
  }

  private def createTempDir(within: Option[File]): File = {
    val dir = tempName(within)
    dir.mkdirs()
    dir
  }

  def using[A](within: Option[File] = None)(f: File => A): A = {
    val dir = createTempDir(within)
    try {
      f(dir)
    } finally {
      FileUtils.deleteDirectory(dir)
    }
  }

  def managed(within: Option[File] = None): Managed[File] =
    sManaged(createTempDir(within))(DirectoryResource)

  def scoped(rs: ResourceScope, within: Option[File] = None): File =
    rs.open(createTempDir(within))(DirectoryResource)
}
