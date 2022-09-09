package com.grey.environment

class LocalSettings {

  // The environment
  val projectDirectory: String = System.getProperty("user.dir")
  val separator: String = System.getProperty("file.separator")

  // Data directory
  val dataDirectory = s"$projectDirectory${separator}data"

}
