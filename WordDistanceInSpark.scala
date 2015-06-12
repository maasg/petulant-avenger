import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD


object WordDistanceInSpark {
  // I'm obviously doing something very wrong, because I get the following exception at the end on the run:
/* ERROR ContextCleaner: Error in cleaning thread
java.lang.InterruptedException
  at java.lang.Object.wait(Native Method)
  at java.lang.ref.ReferenceQueue.remove(ReferenceQueue.java:143)
  at org.apache.spark.ContextCleaner$$anonfun$org$apache$spark$ContextCleaner$$keepCleaning$1.apply$mcV$sp(ContextCleaner.scala:146)
  at org.apache.spark.ContextCleaner$$anonfun$org$apache$spark$ContextCleaner$$keepCleaning$1.apply(ContextCleaner.scala:144)
  at org.apache.spark.ContextCleaner$$anonfun$org$apache$spark$ContextCleaner$$keepCleaning$1.apply(ContextCleaner.scala:144)
  at org.apache.spark.util.Utils$.logUncaughtExceptions(Utils.scala:1618)
  at org.apache.spark.ContextCleaner.org$apache$spark$ContextCleaner$$keepCleaning(ContextCleaner.scala:143)
  at org.apache.spark.ContextCleaner$$anon$3.run(ContextCleaner.scala:65)
ERROR Utils: Uncaught exception in thread SparkListenerBus
java.lang.InterruptedException
  at java.util.concurrent.locks.AbstractQueuedSynchronizer.doAcquireSharedInterruptibly(AbstractQueuedSynchronizer.java:998)
  at java.util.concurrent.locks.AbstractQueuedSynchronizer.acquireSharedInterruptibly(AbstractQueuedSynchronizer.java:1304)
  at java.util.concurrent.Semaphore.acquire(Semaphore.java:312)
  at org.apache.spark.util.AsynchronousListenerBus$$anon$1$$anonfun$run$1.apply$mcV$sp(AsynchronousListenerBus.scala:62)
  at org.apache.spark.util.AsynchronousListenerBus$$anon$1$$anonfun$run$1.apply(AsynchronousListenerBus.scala:61)
  at org.apache.spark.util.AsynchronousListenerBus$$anon$1$$anonfun$run$1.apply(AsynchronousListenerBus.scala:61)
  at org.apache.spark.util.Utils$.logUncaughtExceptions(Utils.scala:1618)
  at org.apache.spark.util.AsynchronousListenerBus$$anon$1.run(AsynchronousListenerBus.scala:60)*/


  
  
  
  type Hit = (Long, String) // (index, the significant word found at index)
  type SameWordInterval = ((Long,Long),String) // ( (startIdx, endIdx), significant word)
  
  /**
   * Finds the distance between given words in the text, defined as 
   * minimum amount of other words in between occurrences of the given words.
   * 
   * Distance between a word and itself is 0.
   * If one of the words does not occur in the text, distance is None
   *  
   * @param textFilePath
   * @param word1
   * @param word2
   * @param sc sparkContext
   * @return
   */
  def wordDistance(textFilePath:String, word1:String, word2:String)(implicit sc:SparkContext):Option[Long] = {
    if (word1 == word2) return Some(0)
    if (word1==null || word2==null) return None
    
    // need access to a cluster to tweak this experimentally, so this is just a guess. 
    // I don't know whether even to fiddle with this at all or just take the defaultParallelism.
    implicit val nbPartitions = sc.defaultParallelism * 2
    
    // would this normally have to be broadcasted to every node then?
    val lines:RDD[String] = sc.textFile(textFilePath, nbPartitions)
    
    // look at text file as sequence of words
    val words = asFlatWordList(lines)
    
    // we filter out all words that are not word1 or word2
    val orderedHits:RDD[Hit] = findHitsInOrder(words, word1, word2)
    
    val smallestDistance:Option[Long] = findSmallestDistanceBetweenDifferentWordHits(orderedHits)
    smallestDistance
  }
  
  def asFlatWordList(lines:RDD[String]):RDD[String] = lines.flatMap { l => l.split(" ") }

  def findHitsInOrder(words:RDD[String], word1:String, word2:String)(implicit nbPartitions:Int):RDD[Hit] = {
    // this part should be embarrassingly parallel, as we like it
    val hits = words.zipWithIndex().filter(p=> (p._1 == word1 || p._1 == word2)).map(p => p.swap)
    // the sort destroys the parallelism, but is necessary for avoiding unnecessary comparisons
    // I'm assuming here that spark implements this sort well, in order to make this still an overall win.
    val sorted = hits.sortByKey(ascending=true, numPartitions = nbPartitions)
    sorted
  }
  
  def findSmallestDistanceBetweenDifferentWordHits(orderedHits:RDD[Hit]): Option[Long] = {
    // println("""***** """+orderedHits.top(100).toList.mkString(", "))
    
    // Our input data is like a pseudo-timeseries, with word indices as time axis, 
    // and only two possible values: word1 or word2.
    // At this point we can still have repetitions of the same word.
    // But a smallest distance can only happen when the signal transitions between hits of word1 and word2 or other other.
    // Our interface allows us to not care about the indices of the transition itself, 
    // only about the index-distance  between the words forming the transition
    // Since we don't care about the transitions, automatic/manual stream fusion should in theory 
    // allow to get rid of the transition-finding as a separate step.
    // But given the time limitation, and because this kind of tweaking might be 
    // much less important than other changes, I'm writing it here as the conceptual sequence of 
    // first finding transitions, and then finding the smallest one.

    // I wish I knew spark well enough already to be able to split the RDD of Hits in partitions
    // which do not break a transition across partitions, in order to avoid a synchronization step.
    // I immediately see two approaches that would work: 
    // 1) If I could include the hit which happens to be part of a signal transition in both partitions, I'd be sure
    // that no transition is broken up across partitions, and then the partitions 
    // would only need to communicate back their winning candidate. seems fail-safe
    // 2) If I could align the partition boundaries to only fall in the middle of hits of the same word
    // we also would not have a broken transition.
    // But if we only do option 2, our number of partitions would depend strongly on the input text.
    
    // Without a solution for avoiding the synchronization, I don't yet have a sense for how much of an improvement 
    // it gives to do this part of the computation on the cluster instead of in the driver.
    // For a non-pathological case, the filtering all words to just our word1 and word2 should already be 
    // a huge shrink in dataset size.
    
    // but since there was a big warning against the trivial "make it run on spark" 
    // implementation of immediately collecting, I'll try to write it in a spark-compatible style.
    // 
    // Stylewise, I'm unhappy because it becomes a bit car/cdr soup though, 
    // which could be solved by defining case classes.

    // I can't find a spark foldLeft which lets me fold Hits into SameWordIntervals.
    // A verbose workaround but pretty foolproof solution would be to create a trait as union type.
    // In this case I took the "wrap everything as a list" approach. Pretty ugly.
    // I'm not sure whether defining a custom accumulator would help for making it less ugly 
    // and for making it more performant.
     
    val reverseOrderedSameWordIntervals:List[SameWordInterval] = convertHitsToSameWordIntervals(orderedHits)
    // the fact that I do a fold which is an action instead of a transform, 
    // is bumping me out of RDDs into regular scala-land at this point anyway.
    // At this point we have had another huge data shrink from skipping consecutive hits of the same word,
    // and it is already late, so I'm going to do the last part in pure scala
    
    // println("""***** """+reverseOrderedSameWordIntervals.take(100).toList.mkString(", "))
    
    // if one of the words does not exist, there will only be one element in this list
    if (reverseOrderedSameWordIntervals.size == 1) {
      None 
    } else {
      Some(findMinimumDistanceBetweenAlternatingIntervals(reverseOrderedSameWordIntervals))
    }
  }
   
  def convertHitsToSameWordIntervals(orderedHits:RDD[Hit]):List[SameWordInterval] = {
    // a hit becomes an interval with same start as end
    orderedHits.map(hit => List(((hit._1,hit._1), hit._2))).
      fold(List[SameWordInterval]())(WordDistanceInSpark.foldIntoExistingSameWordIntervals _)
  }
  
  def foldIntoExistingSameWordIntervals(acc:List[SameWordInterval], extra:List[SameWordInterval]): List[SameWordInterval] = {
    // warning: car/cdr soup ahead. Which is why this part is unittested and gets a post-condition.
    def wordAt(swi: SameWordInterval):String = swi._2
    
    //println("* Adding "+extra +" into "+acc)
    
    var shouldMerge = false
    val result = if (acc.isEmpty) { 
      extra
    } else {
        val endOfAcc = acc.head
        val beginOfExtra = extra.last
        shouldMerge = wordAt(endOfAcc)==wordAt(beginOfExtra)
        if (shouldMerge) {
          val mergedLink = ((acc.head._1._1, extra.last._1._2), wordAt(endOfAcc))
          extra.dropRight(1):::(mergedLink::acc.tail)
        } else {
          extra:::acc
        }
    }

    val expectedSize = acc.size + extra.size - (if (shouldMerge) 1 else 0)
    if (result.size != expectedSize) throw new Exception("Programming error in merging "+extra +" into acc "+acc+": wrong size of result "+result)
    result
  }
  
  def findMinimumDistanceBetweenAlternatingIntervals(reverseOrderedSameWordIntervals:List[SameWordInterval]):Long = {
    // in pure scala I get sliding, in spark this seems to be part of the MLLib. I have not investigated further.
    val distances = reverseOrderedSameWordIntervals.sliding(2).map {
        case laterInterval::precedingInterval::Nil => calcDist(precedingInterval, laterInterval)
      }
    distances.min
  }

  def calcDist(first:SameWordInterval, second:SameWordInterval):Long = calcDist(first._1._2, second._1._1)
  
  def calcDist(first:Long, last:Long):Long = last-first-1
  
  def main(args:Array[String]):Unit = {
    testFoldHandlesFirstHit()
    testFoldHandlesSequenceOfSameWords()
    testFoldHandlesSequenceOfDifferentWords()
    testFoldHandlesIntermediateMergingCase()
    

    val conf = new SparkConf().setAppName("WordDistanceInSpark").setMaster("local")
    implicit val sc = new SparkContext(conf)
    
    // practical test 1 runs on the readme file of the spark-notebook itself.
    val testFileREADME = "README.md" 
    val minSparkToVersion = wordDistance(testFileREADME, "Spark", "version")
    println("distance between Spark and version : "+minSparkToVersion)
    if (Some(0L) != minSparkToVersion) {
      throw new Exception("""Distance between "Spark" and "version" found to be """+minSparkToVersion+
          """. However, this was working on Jelle's machine on June 12th on the README from spark-notebook/master.""")
    }
    
    // practical test 2 runs on the well-tokenized shakespeare corpus, 
    // which is downloadable at http://norvig.com/ngrams/shakespeare.txt
    val testFileShakespeare = "shakespeare.txt"
    val minRomeoToJuliet = wordDistance(testFileShakespeare, "Romeo", "Juliet")
    println("distance between Romeo and Juliet : "+minRomeoToJuliet)
    // excerpt from shakespeare.txt: Is father , mother , Tybalt , Romeo , Juliet ,
    if (Some(1L) != minRomeoToJuliet) {
      throw new Exception("""Distance between "Romeo" and "Juliet found to be """+minRomeoToJuliet+
          """. However, this was working on Jelle's machine on June 12th.""")
    }
    
    sc.stop()
  }
  
  def testFoldHandlesFirstHit() {
    val expected = List(((1,4),"Foo"))
    val actual = foldIntoExistingSameWordIntervals(List(), List(((1,4),"Foo")))
    if (expected != actual) throw new Exception(actual.toString())
  }
  
  def testFoldHandlesSequenceOfSameWords() {
    val expected = List( ((3,7)->"Bar"), ((1,1)->"Foo") )
    val actual = foldIntoExistingSameWordIntervals(List(((3,3),"Bar"),((1,1),"Foo")), List(((7,7),"Bar")))
    if (expected != actual) throw new Exception(actual.toString())
  }
  
  def testFoldHandlesSequenceOfDifferentWords() {
    val expected = List( ((3,3)->"Bar"),  ((1,1)->"Foo") )
    val actual = foldIntoExistingSameWordIntervals(List( ((1,1),"Foo") ), List(((3,3),"Bar")))
    if (expected != actual) throw new Exception(actual.toString())
  }
  
  def testFoldHandlesIntermediateMergingCase() {
    val expected = List( ((10,10), "Foo"), ((3,7),"Bar"), ((1,1),"Foo") )
    val actual = foldIntoExistingSameWordIntervals(acc=List( ((3,3), "Bar"), ((1,1),"Foo")), extra=List( ((10,10),"Foo"), ((5,7),"Bar")))
    if (expected != actual) throw new Exception(actual.toString())
  }
}

