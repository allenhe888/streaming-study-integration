package com.bigdata.scalastudy.basic

import org.junit.Test

class BasicLanguageDemo {

  @Test
  def testWhileLoop(): Unit ={

    var num = 10
    var x = 0
    while (x<10){
      num += x
      x+= 1
    }

    println(x)
    println(num)

  }

}
