package com.utils

import com.google.gson.Gson
import com.main.Message

object MessageUtils {
  def parseFromJson(lines: Iterator[String]): Iterator[Message] = {
    val gson = new Gson
    lines.map(line => gson.fromJson(line, classOf[Message]))
  }
}
