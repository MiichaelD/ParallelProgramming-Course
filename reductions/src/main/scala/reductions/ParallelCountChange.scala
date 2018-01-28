package reductions

import org.scalameter._
import common._

object ParallelCountChangeRunner {

  @volatile var seqResult = 0
  @volatile var seqResult2 = 0
  @volatile var parResult = 0

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 20,
    Key.exec.maxWarmupRuns -> 40,
    Key.exec.benchRuns -> 80,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val amount = 250 // 4
    val coins = List(1, 2, 5, 10, 20, 50) // List(1, 2, 3)
    val seqtime = standardConfig measure {
      seqResult = ParallelCountChange.countChangeRec(amount, coins)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential count time: $seqtime ms")

    val seqtime2 = standardConfig measure {
      seqResult2 = ParallelCountChange.countChangeRec(amount, coins)
    }
    println(s"sequential result recursive = $seqResult2")
    println(s"sequential count time: $seqtime2 ms")

    def measureParallelCountChange(threshold: ParallelCountChange.Threshold): Unit = {
      val fjtime = standardConfig measure {
        parResult = ParallelCountChange.parCountChange(amount, coins, threshold)
      }
      println(s"parallel result = $parResult")
      println(s"parallel count time: $fjtime ms")
      println(s"speedup: ${seqtime / fjtime}")
    }

    measureParallelCountChange(ParallelCountChange.moneyThreshold(amount))
    measureParallelCountChange(ParallelCountChange.totalCoinsThreshold(coins.length))
    measureParallelCountChange(ParallelCountChange.combinedThreshold(amount, coins))
  }
}

object ParallelCountChange {

  /** Returns the number of ways change can be made from the specified list of
   *  coins for the specified amount of money.
   */
  def countChangeRec(money: Int, coins: List[Int], coinIndex: Int = 0): Int = {
    if (money == 0)
      return 1

    if (money < 0 || coins.lengthCompare(coinIndex) == 0) {
      return 0
    }

    val withCurrentCoins = countChangeRec(money - coins.apply(coinIndex), coins, coinIndex)
    val withoutCurrentCoin = countChangeRec(money, coins, coinIndex + 1)

    withCurrentCoins + withoutCurrentCoin
  }


  def countChange(money: Int, coins: List[Int]): Int = {
    if (money < 0) {
      return 0
    }

    if (money == 0) {
      return 1
    }

    var memo = Array.fill(money + 1)(0)
    memo.update(0, 1)

    for (coin <- coins) {
      var index = coin
      while (index <= money) {
        memo.apply(index) += memo.apply(index - coin)
        index += 1
      }
    }
    memo.apply(money)
  }

  type Threshold = (Int, List[Int]) => Boolean

  /** In parallel, counts the number of ways change can be made from the
   *  specified list of coins for the specified amount of money.
   */
  def parCountChange(money: Int, coins: List[Int], threshold: Threshold): Int = {
    if (money <= 0 || coins.isEmpty || threshold(money, coins)) {
      return countChangeRec(money, coins)
    }

    val (left, right) = parallel(parCountChange(money - coins.head, coins, threshold), parCountChange(money, coins.tail, threshold))
    left + right
  }

  /** Threshold heuristic based on the starting money. */
  def moneyThreshold(startingMoney: Int): Threshold =
    //(money, coins) => money <= startingMoney // no speedup
    (money, coins) => money <= 2 * startingMoney / 3 // good for recursive solutions


  /** Threshold heuristic based on the total number of initial coins. */
  def totalCoinsThreshold(totalCoins: Int): Threshold =
     //(money, coins) => coins.lengthCompare(totalCoins) <= 0 // no speedup
    (money, coins) => coins.lengthCompare(2 * totalCoins / 3) <= 0 // good for recursive solutions


  /** Threshold heuristic based on the starting money and the initial list of coins. */
  def combinedThreshold(startingMoney: Int, allCoins: List[Int]): Threshold = {
    //(money, coins) => moneyThreshold(startingMoney)(money, coins) && totalCoinsThreshold(allCoins.length)(money, coins)
    (money, coins) => money * coins.length <= (startingMoney * allCoins.length / 2)
  }
}
