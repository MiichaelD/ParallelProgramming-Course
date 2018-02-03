package kmeans

import scala.annotation.tailrec
import scala.collection._
import scala.util.Random
import org.scalameter._
import common._

class KMeans {

  def generatePoints(k: Int, num: Int): Seq[Point] = {
    val randx = new Random(1)
    val randy = new Random(3)
    val randz = new Random(5)
    (0 until num)
      .map({ i =>
        val x = ((i + 1) % k) * 1.0 / k + randx.nextDouble() * 0.5
        val y = ((i + 5) % k) * 1.0 / k + randy.nextDouble() * 0.5
        val z = ((i + 7) % k) * 1.0 / k + randz.nextDouble() * 0.5
        new Point(x, y, z)
      }).to[mutable.ArrayBuffer]
  }

  def initializeMeans(k: Int, points: Seq[Point]): Seq[Point] = {
    val rand = new Random(7)
    (0 until k).map(_ => points(rand.nextInt(points.length))).to[mutable.ArrayBuffer]
  }

  def findClosest(p: Point, means: GenSeq[Point]): Point = {
    assert(means.nonEmpty)
    var minDistance = p.squareDistance(means.head)
    var closest = means.head
    var i = 1
    while (i < means.length) {
      val distance = p squareDistance means(i)
      if (distance < minDistance) {
        minDistance = distance
        closest = means(i)
      }
      i += 1
    }
    closest
  }

  /**
    * Takes a generic sequence of points and a generic sequence of means. It returns a generic map collection, which
    * maps each mean to the sequence of points in the corresponding cluster.
    *
    * Hint: Use groupBy and the findClosest method, which is already defined for you. After that, make sure that all the
    * means are in the GenMap, even if their sequences are empty.
    */
  def classify(points: GenSeq[Point], means: GenSeq[Point]): GenMap[Point, GenSeq[Point]] = {
    val closestMap = points.par.groupBy(findClosest(_, means))  // group points to closest means in parallel.
    means.par.map(mean => mean -> closestMap.getOrElse(mean, GenSeq())).toMap  // create a map with each mean -> points
  }

  def findAverage(oldMean: Point, points: GenSeq[Point]): Point = if (points.isEmpty) oldMean else {
    var x = 0.0
    var y = 0.0
    var z = 0.0
    points.seq.foreach { p =>
      x += p.x
      y += p.y
      z += p.z
    }
    new Point(x / points.length, y / points.length, z / points.length)
  }

  /**
    * Returns the new sequence of means. It takes the map of classified points produced in the previous step, and the
    * sequence of previous means. It takes care of  preserve order in the resulting generic sequence -- the mean i in
    * the resulting sequence must correspond to the mean i from oldMeans.
    *
    * Hint: Make sure you use the findAverage method that is predefined for you.
    */
  def update(classified: GenMap[Point, GenSeq[Point]], oldMeans: GenSeq[Point]): GenSeq[Point] = {
    // Update the old means map to a (probably) new average of the same points it groups.
    oldMeans.par.map(oldMean => findAverage(oldMean, classified(oldMean)))
  }

  /**
    * Takes a sequence of old means and the sequence of updated means, and returns a boolean indicating if the algorithm
    * converged or not. Given an eta parameter, oldMeans and newMeans, it returns true if the algorithm converged, and
    * false otherwise.
    *
    * The algorithm converged iff the square distance between the old and the new mean is less than or equal to eta,
    * for all means.
    *
    * Note: the means in the two lists are ordered -- the mean at i in oldMeans is the previous value of the mean at i
    * in newMeans.
    */
  def converged(eta: Double)(oldMeans: GenSeq[Point], newMeans: GenSeq[Point]): Boolean = {
    val combinedSeq : GenSeq[(Point, Point)] = oldMeans zip newMeans // zip combines lists/sequences.
    combinedSeq.forall({case (oldMean, newMean) => (oldMean squareDistance newMean) <= eta})
  }

  /**
    * Returns the sequence of means, each corresponding to a specific cluster. This method takes a sequence of points
    * points, previously computed sequence of means means, and the eta value.
    *
    * Hint: kMeans implements the steps 2-4 from the K-means pseudocode.
    */
  @tailrec
  final def kMeans(points: GenSeq[Point], means: GenSeq[Point], eta: Double): GenSeq[Point] = {
    val newMeans = update(classify(points, means), means)
    // Implementation needs to be tail recursive
    if (!converged(eta)(means, newMeans)) { kMeans(points, newMeans, eta) } else newMeans
  }
}

/** Describes one point in three-dimensional space.
 *
 *  Note: deliberately uses reference equality.
 */
class Point(val x: Double, val y: Double, val z: Double) {
  private def square(v: Double): Double = v * v
  def squareDistance(that: Point): Double = {
    square(that.x - x)  + square(that.y - y) + square(that.z - z)
  }
  private def round(v: Double): Double = (v * 100).toInt / 100.0
  override def toString = s"(${round(x)}, ${round(y)}, ${round(z)})"
}


object KMeansRunner {

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 20,
    Key.exec.maxWarmupRuns -> 40,
    Key.exec.benchRuns -> 25,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]) {
    val kMeans = new KMeans()

    val numPoints = 500000
    val eta = 0.01
    val k = 32
    val points = kMeans.generatePoints(k, numPoints)
    val means = kMeans.initializeMeans(k, points)

    val seqtime = standardConfig measure {
      kMeans.kMeans(points, means, eta)
    }
    println(s"sequential time: $seqtime ms")

    val partime = standardConfig measure {
      val parPoints = points.par
      val parMeans = means.par
      kMeans.kMeans(parPoints, parMeans, eta)
    }
    println(s"parallel time: $partime ms")
    println(s"speedup: ${seqtime / partime}")
  }

}
