package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    var count: Int = 0
    for (c <- chars) {
      if (c == '(') {
        count += 1
      }
      if (c == ')') {
        count -= 1
      }

      if (count < 0) {
        return false
      }
    }
    return count == 0
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(idx: Int, until: Int, arg1: Int = 0, arg2: Int = 0): (Int, Int) = {
      var index = idx
      var start = arg1
      var end = arg2
      while (index < until) {
        var belowZero = start < 0
        if (chars(index) == '(') {
          if (belowZero) end += 1 else start+= 1
        } else if (chars(index) == ')') {
          if (belowZero) end -= 1 else start -= 1
        }
        index += 1
      }
      (start, end)
    }

    def reduce(from: Int, until: Int): (Int, Int) = {
      if (until - from <= threshold) {
        return traverse(from, until)
      }

      val midpoint = from + (until - from) / 2
      val (red1, red2) = parallel(reduce(from, midpoint), reduce(midpoint, until))

      if (red1._1 < 0 && red2._1 > 0) {           // if reduce1.start < 0 and reduce2.start > 0
        (red1._1, red2._1 + red1._2 + red2._2)
      } else if (red2._1 < 0 && red2._2 > 0) {    // if reduce1.start < 0 and reduce2.end > 0
        (red1._1 + red2._1 + red1._2, red2._2)    //
      } else {
        (red1._1 + red2._1, red1._2 + red2._2)    // reduces' starts, ends
      }
    }

    reduce(0, chars.length) == (0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
