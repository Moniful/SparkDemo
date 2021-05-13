package Scala

/**
 * @author 虚竹
 * @date 2021/5/7 17:49
 * @version 1.0
 */
object Implicit {
//    implicit val str: String = "hello world!"

    def hello(implicit arg: String="good bey world!"): Unit = {
        println(arg)
    }

    def hello1(str:String) ={
        print(str)
    }

    def main(args: Array[String]): Unit = {
        hello
    }


}
