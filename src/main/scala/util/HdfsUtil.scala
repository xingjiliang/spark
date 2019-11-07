package util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

object HdfsUtil {
  def deleteIfExists(hadoopConfiguration: Configuration, pathString: String): Boolean = {
    val path = new Path(pathString)
    val fileSystem = path.getFileSystem(hadoopConfiguration)
    if (fileSystem.exists(path)) {
      return fileSystem.delete(path, true)
    }
    return false
  }
}
