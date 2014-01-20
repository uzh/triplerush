object Test {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(57); 
  println("Welcome to the Scala worksheet");$skip(66); 
  val abc = List((1,2,3),(4,5,6),(7,8,9),(1,2,3),(4,5,6),(7,8,9));System.out.println("""abc  : List[(Int, Int, Int)] = """ + $show(abc ));$skip(119); 
  val boundVars = abc.foldLeft(List[Int]())((listAppend, tp) => (tp._1 :: tp._2 :: listAppend).filter(_ < 6)).distinct;System.out.println("""boundVars  : List[Int] = """ + $show(boundVars ));$skip(108); 
  val boundVars1 = abc.foldLeft(Set[Int]())((listAppend, tp) => (listAppend + tp._1 + tp._2)).filter(_ < 6);System.out.println("""boundVars1  : scala.collection.immutable.Set[Int] = """ + $show(boundVars1 ))}
  }
