package com.wadejensen.jacksonscalaexample

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

class Main extends App {

  override def main(args: Array[String]): Unit = {

    val mapper = new ObjectMapper
    mapper.registerModule(DefaultScalaModule)



    ???
  }
}
