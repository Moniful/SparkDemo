package Scala

/**
 * @author 虚竹
 * @date 2021/5/7 16:45
 * @version 1.0
 */
object ScalaTest {
    def main(args: Array[String]): Unit = {
        val list: List[Int] = List(1, 2, 3, 4, 555, 12, 22)
        val list2 = List(List(1, 2, 3, 4), List(5, 6, 7, 8))
        val list3 = List("x", "y", "z")
        println(list.filter(x => x >= 12))
        println(list.map(x => x + 1))
        println(list2.flatten)
        println(list3.flatMap(x => x.toUpperCase()))
        println(list.reduce((x, y) => x - y))
        val word = List("hello world hello leqee")
        println(word.flatMap(x => x.split(" "))
            .map(x => (x, 1))
            .groupBy(_._1)
            .map(x => (x._1,x._2.size)))


    }

}
