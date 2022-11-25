object Appz {
  def tax(year: Double, premium: Double, compensation: Double): Double ={
    (year + (premium / year) + compensation) * 0.13
  }
  def insert[T](list: List[T], i: Int, value: T) = {
    list.take(i) ++ List(value) ++ list.drop(i)
  }
  def main(args: Array[String]): Unit = {
    println("Task A:")
    var s: String = "Hello, Scala!"
    var lst: List[Double] = Nil
    lst =  List(100, 150, 200, 80, 120, 75)

    println(s.reverse)
    println(s.toLowerCase)
    println(s.dropRight(1))
    s = s.concat("and goodbye python!")
    println(s)

    println("Task B:")
    println(tax(1000,10,500))
    var sum: Double = 0

    println("Task C:")
    lst.foreach((el)=>sum = sum + tax(el, 10,500))
    val avg: Double = sum / lst.length
    val avs = new Array[Double](6)
    for (i <- lst.indices) {
      val x: Double = tax(lst(i), 10, 500)
      avs(i) = (avg - x) / x * 100
    }
    avs.foreach(println)

    println("Task D:")
    lst = lst.updated(4,lst(4) + 10).sorted
    lst.foreach(println)

    println("Task E:")
    lst = lst :+ 350.0 :+ 90.0
    lst = lst.sorted
    lst.foreach(println)

    println("Task F:")
    var index_add: Integer = -1
    val new_emp = 130
    index_add = lst.indexWhere(_ >= new_emp)
    if (index_add == -1) {
      index_add = lst.length
    }
    lst = insert(lst, index_add, new_emp)
    lst.foreach(println)

    println("Task G:")
    val middle_min = 100
    val middle_max = 170
    var middleIdsList: List[Int] = Nil
    for (i <- lst.indices) {
        if (lst(i) >= middle_min & lst(i) <= middle_max) {
          middleIdsList = middleIdsList :+ i
        }
    }
    middleIdsList.foreach(println)

    println("Task H:")
    lst = lst.map(_ * 1.07).map(_ * 100).map(Math.round(_)).map(_/100.0)
    lst.foreach(println)


  }
}
