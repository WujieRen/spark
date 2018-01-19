Spark2.10中使用累加器、注意点以及实现自定义累加器

Reference:
    http://blog.csdn.net/u013468917/article/details/70617085

累加器（accumulator）是Spark中提供的一种分布式的变量机制，其原理类似于mapreduce，即分布式的改变，然后聚合这些改变。累加器的一个常见用途是在调试时对作业执行过程中的事件进行计数。
累加器简单使用

Spark内置的提供了Long和Double类型的累加器。下面是一个简单的使用示例，在这个例子中我们在过滤掉RDD中奇数的同时进行计数，最后计算剩下整数的和。

[java] view plain copy

    val sparkConf = new SparkConf().setAppName("Test").setMaster("local[2]")  
    val sc = new SparkContext(sparkConf)  
    val accum = sc.longAccumulator("longAccum") //统计奇数的个数  
    val sum = sc.parallelize(Array(1,2,3,4,5,6,7,8,9),2).filter(n=>{  
      if(n%2!=0) accum.add(1L)   
      n%2==0  
    }).reduce(_+_)  
      
    println("sum: "+sum)  
    println("accum: "+accum.value)  
      
    sc.stop()  


结果为：

sum: 20
accum: 5

这是结果正常的情况，但是在使用累加器的过程中如果对于spark的执行过程理解的不够深入就会遇到两类典型的错误：少加（或者没加）、多加。

少加的情况：

对于如下代码：

[java] view plain copy

    val accum = sc.longAccumulator("longAccum")  
    val numberRDD = sc.parallelize(Array(1,2,3,4,5,6,7,8,9),2).map(n=>{  
      accum.add(1L)  
      n+1  
    })  
    println("accum: "+accum.value)  



执行完毕，打印的值是多少呢？答案是0，因为累加器不会改变spark的lazy的计算模型，即在打印的时候像map这样的transformation还没有真正的执行，从而累加器的值也就不会更新。

多加的情况：

对于如下代码：

[java] view plain copy

    val accum = sc.longAccumulator("longAccum")  
    val numberRDD = sc.parallelize(Array(1,2,3,4,5,6,7,8,9),2).map(n=>{  
      accum.add(1L)  
      n+1  
    })  
    numberRDD.count  
    println("accum1:"+accum.value)  
    numberRDD.reduce(_+_)  
    println("accum2: "+accum.value)  


结果我们得到了：

accum1:9

accum2: 18

我们虽然只在map里进行了累加器加1的操作，但是两次得到的累加器的值却不一样，这是由于count和reduce都是action类型的操作，触发了两次作业的提交，所以map算子实际上被执行了了两次，在reduce操作提交作业后累加器又完成了一轮计数，所以最终累加器的值为18。究其原因是因为count虽然促使numberRDD被计出来，但是由于没有对其进行缓存，所以下次再次需要使用numberRDD这个数据集是，还需要从并行化数据集的部分开始执行计算。解释到这里，这个问题的解决方法也就很清楚了，就是在count之前调用numberRDD的cache方法（或persist），这样在count后数据集就会被缓存下来，reduce操作就会读取缓存的数据集而无需从头开始计算了。改成如下代码即可：

[java] view plain copy

    val accum = sc.longAccumulator("longAccum")  
    val numberRDD = sc.parallelize(Array(1,2,3,4,5,6,7,8,9),2).map(n=>{  
      accum.add(1L)  
      n+1  
    })  
    numberRDD.cache().count  
    println("accum1:"+accum.value)  
    numberRDD.reduce(_+_)  
    println("accum2: "+accum.value)  


这次两次打印的值就会保持一致了。

自定义累加器
自定义累加器类型的功能在1.X版本中就已经提供了，但是使用起来比较麻烦，在2.0版本后，累加器的易用性有了较大的改进，而且官方还提供了一个新的抽象类：AccumulatorV2来提供更加友好的自定义类型累加器的实现方式。官方同时给出了一个实现的示例：CollectionAccumulator类，这个类允许以集合的形式收集spark应用执行过程中的一些信息。例如，我们可以用这个类收集Spark处理数据时的一些细节，当然，由于累加器的值最终要汇聚到driver端，为了避免 driver端的outofmemory问题，需要对收集的信息的规模要加以控制，不宜过大。
实现自定义类型累加器需要继承AccumulatorV2并至少覆写下例中出现的方法，下面这个累加器可以用于在程序运行过程中收集一些文本类信息，最终以Set[String]的形式返回。
[java] view plain copy

    import java.util  
      
    import org.apache.spark.util.AccumulatorV2  
      
    class LogAccumulator extends AccumulatorV2[String, java.util.Set[String]] {  
      private val _logArray: java.util.Set[String] = new java.util.HashSet[String]()  
      
      override def isZero: Boolean = {  
        _logArray.isEmpty  
      }  
      
      override def reset(): Unit = {  
        _logArray.clear()  
      }  
      
      override def add(v: String): Unit = {  
        _logArray.add(v)  
      }  
      
      override def merge(other: AccumulatorV2[String, java.util.Set[String]]): Unit = {  
        other match {  
          case o: LogAccumulator => _logArray.addAll(o.value)  
        }  
      
      }  
      
      override def value: java.util.Set[String] = {  
        java.util.Collections.unmodifiableSet(_logArray)  
      }  
      
      override def copy(): AccumulatorV2[String, util.Set[String]] = {  
        val newAcc = new LogAccumulator()  
        _logArray.synchronized{  
          newAcc._logArray.addAll(_logArray)  
        }  
        newAcc  
      }  
    }  


测试类：

[java] view plain copy

    import scala.collection.JavaConversions._  
      
    import org.apache.spark.{SparkConf, SparkContext}  
      
    object Main {  
      def main(args: Array[String]): Unit = {  
        val sparkConf = new SparkConf().setAppName("Test").setMaster("local[2]")  
        val sc = new SparkContext(sparkConf)  
        val accum = new LogAccumulator  
        sc.register(accum, "logAccum")  
        val sum = sc.parallelize(Array("1", "2a", "3", "4b", "5", "6", "7cd", "8", "9"), 2).filter(line => {  
          val pattern = """^-?(\d+)"""  
          val flag = line.matches(pattern)  
          if (!flag) {  
            accum.add(line)  
          }  
          flag  
        }).map(_.toInt).reduce(_ + _)  
      
        println("sum: " + sum)  
        for (v <- accum.value) print(v + " ")  
        println()  
        sc.stop()  
      }  
    }  


本例中利用自定义的收集器收集过滤操作中被过滤掉的元素，当然这部分的元素的数据量不能太大。运行结果如下：
sum; 32
7cd 4b 2a 