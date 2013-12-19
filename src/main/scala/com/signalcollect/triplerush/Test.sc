package com.signalcollect.triplerush
import scala.util.Random

object Test {
  println("Welcome to the Scala worksheet")       //> Welcome to the Scala worksheet
  //val rnd = new scala.util.Random
	//val range = 100 to 150
	println((100 to 200)(Random.nextInt(100 to 200 length)))
                                                  //> 124
	println(Random.nextInt(100 to 200 length))//> 89
	def prn(x: Int*) = println(x)             //> prn: (x: Int*)Unit
	prn(3, 4, 5, 6)                           //> WrappedArray(3, 4, 5, 6)
}