package com.signalcollect.triplerush
import scala.util.Random

object Test {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(120); 
  println("Welcome to the Scala worksheet");$skip(120); 
  //val rnd = new scala.util.Random
	//val range = 100 to 150
	println((100 to 200)(Random.nextInt(100 to 200 length)));$skip(44); 
	println(Random.nextInt(100 to 200 length));$skip(31); 
	def prn(x: Int*) = println(x);System.out.println("""prn: (x: Int*)Unit""");$skip(17); 
	prn(3, 4, 5, 6)}
}
