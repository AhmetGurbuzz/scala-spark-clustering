package ee.ut.cs

import scala.math.{min, pow, sqrt}
import scala.collection.mutable.ListBuffer

class DBSCANScala(val data:Seq[Seq[Double]], val eps:Double, val minPts:Int) {
  private val _isVisited:ListBuffer[Boolean] = ListBuffer.fill[Boolean](data.length)(false)
  private val _noiseList:ListBuffer[Int] = ListBuffer[Int]()
  private val _clusters:ListBuffer[Array[Int]] = ListBuffer[Array[Int]]() // Clusters

  def isVisited:ListBuffer[Boolean] = _isVisited
  def noiseList:ListBuffer[Int] = _noiseList
  def clusters:ListBuffer[Array[Int]] = _clusters

  def DBSCAN(/*, eps: Double, minPts: Int*/) = {
    val dataIndex = data.indices.toArray // (0,1,2,....) data row index

    /// For each P
    dataIndex.foreach { P =>
      if (!_isVisited(P)) {
        _isVisited(P) = true /// mark P as visited
        val NeighborPts = regionQuery(P, eps, dataIndex)
        if (NeighborPts.length < minPts)
          _noiseList.+=(P) /// "mark P as Noise" or anomaly
        else {
          val C = ListBuffer[Int]() //next cluster
          expandCluster(P, NeighborPts, C, eps, minPts, dataIndex)
          println("A cluster found",C)
          _clusters.+=(C.toArray)
        }
      }
    }

  }

  def expandCluster(P: Int, NeighborPts: ListBuffer[Int], C: ListBuffer[Int], eps: Double, minPts: Double, dataIndex: Array[Int]) = {
    C.+=(P) // add P to cluster C
    var itr:Int = 0
    while (itr < NeighborPts.length) { //for each point P' in NeighborPts
      val p_ = NeighborPts(itr)
      if (!_isVisited(p_)) { //if P' is not visited
        _isVisited(p_) = true
        val NeighborPts_ = regionQuery(p_, eps, dataIndex)
        if (NeighborPts_.length >= minPts) { /// NeighborPts= NeighborPts joined with NeighborPts'
          for (i <- NeighborPts_) {
            if (!NeighborPts.contains(i))
              NeighborPts.+=(i)
          }
        }
      }
      if (!isMemberOfAnyCluster(p_)) { //if P' is not yet member of any cluster
        if(!C.contains(p_)){
          C.+=(p_)
        }
      }
      itr = itr + 1
    }
  }

  def regionQuery(P: Int, eps: Double, dataIndex: Array[Int]) = {
    val allPoints = ListBuffer[Int]()
    dataIndex.map(i => {
      if (distanceSqrt(data(i), data(P)) <= eps) {
        allPoints.+=(i)
      }
    })
    allPoints
  }

  def distanceSqrt(p1: Seq[Double], p2: Seq[Double]) = {
    sqrt(pow(p1(0) - p2(0), 2.0) + pow(p1(1) - p2(1), 2.0))
  }

  def isMemberOfAnyCluster(p_ : Int): Boolean = {
    if (_clusters.isEmpty)
      return false

    _clusters.flatten.contains(p_) // Array(Array(1,2),Array(3,4)) flatten => Array(1,2,3,4)
  }
}

object Test extends App{
  val data = Seq(
    Seq(1.1, 1.2), //   ->0 
    Seq(6.1, 4.8), //1
    Seq(0.1, 0.1), //2
    Seq(9.0, 9.0), // 3 ->0 
    Seq(9.1, 9.1), // 4 ->0 
    Seq(0.4, 2.1), //5
    Seq(9.2, 9.2), // 6 ->0 
    Seq(1.0, 1.1), //   7 ->1 
    Seq(0.9, 1.0), //   8 ->1 
    Seq(0.9, 0.75) //   9 ->1 
  )

  val dbscan = new DBSCANScala(data,0.25, 3)

  dbscan.DBSCAN()

  dbscan.clusters.foreach(l => println(l.toList))

  val map = dbscan.clusters
    .zipWithIndex
    .flatMap(l => l._1.map(k => (k -> l._2)))
    .toMap

  val clusterNo = for(i <- data.indices) yield { map.getOrElse(i, -1).toByte } // -1

  println(clusterNo)
}