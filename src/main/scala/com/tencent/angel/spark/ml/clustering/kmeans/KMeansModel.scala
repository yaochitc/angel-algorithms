package com.tencent.angel.spark.ml.clustering.kmeans

import com.tencent.angel.ml.clustering.kmeans.KMeansModel._
import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.{IntFloatVector, Vector}
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.spark.models.PSMatrix
import org.apache.spark.rdd.RDD

object KMeansModel {
  val KMEANS_CENTERS_MAT = "kmeans_centers"
  val KMEANS_V_MAT = "kmeans_v"
  val KMEANS_OBJ = "kmeans_obj"
}


class KMeansModel(ks: Array[Int]) extends Serializable {
  private val indexRange: Long = SharedConf.indexRange

  private val K = ks.sum

  var lcCenters: Array[Vector] = Array()
  var lcV: IntFloatVector = VFactory.denseFloatVector(K)
  val centerDist = new Array[Double](K)

  // Reference for centers matrix on PS server
  val centers: PSMatrix = PSMatrix.matrix(new MatrixContext(KMEANS_CENTERS_MAT, K, indexRange, -1, -1, -1, SharedConf.modelType))
  val v: PSMatrix = PSMatrix.matrix(new MatrixContext(KMEANS_CENTERS_MAT, 1, K, -1, -1, -1, RowType.T_FLOAT_DENSE))


  def train(trainData: RDD[LabeledData], param: Param): Unit = {
    for (i <- ks.indices) {
      val k = ks(i)
      initKCentersRandomly(trainData, k)

      for (epoch <- 1 to param.numEpoch) {
        val startEpoch = System.currentTimeMillis()
        trainOneEpoch(epoch, trainData, i, k)
        val epochTime = System.currentTimeMillis() - startEpoch
      }
    }
  }

  def initKCentersRandomly(trainData: RDD[LabeledData], k: Int): Unit = {
    val points = trainData.takeSample(false, k)
    for (i <- 0 until k) {
      val newCent = points(i).getX
      newCent.setRowId(i)
      centers.increment(newCent)
    }
  }

  /**
    * Each epoch updation is performed in three steps. First, pull the centers updated by last
    * epoch from PS. Second, a mini batch of size batch_sample_num is sampled. Third, update the
    * centers in a mini-batch way.
    *
    * @param epoch     : the epoch number
    * @param trainData : the trainning data storage
    * @param i         : the index of k
    * @param k         : number of clusters
    */
  def trainOneEpoch(epoch: Int, trainData: RDD[LabeledData], i: Int, k: Int): Unit = {
    val offset = ks.slice(0, i).sum
    val indices = (offset to offset + k).toArray

    pullCentersFromPS(indices)
    val lcV = v.pull(0, indices)

    val oldCenters = Array.ofDim[Vector](k)
    (0 until k).foreach { i =>
      oldCenters(i) = lcCenters(i).copy()
    }

    val oldVs = lcV.copy()

    updateCenters(oldCenters, oldVs, k)
  }

  /**
    * Pull centers from PS to local worker
    */
  def pullCentersFromPS(indices: Array[Int]): Unit = {
    lcCenters = centers.pull((0 until K).toArray)

    centerDist.indices.foreach { i =>
      val c = lcCenters(i)
      centerDist(i) = c.dot(c)
    }
  }


  /**
    * Calculate the distance between instance and centers, and find the closest center
    *
    * @param x : a instance
    * @return : the closest center id
    */
  def findClosestCenter(x: Vector, k: Int): (Int, Double) = {
    var minDis = Double.MaxValue
    var clstCent: Int = -1

    val len2 = x.dot(x)
    for (i <- 0 until K) {
      val dist = centerDist(i) - 2 * lcCenters(i).dot(x) + len2
      if (dist < minDis) {
        minDis = dist
        clstCent = i
      }
    }
    (clstCent, Math.sqrt(minDis))
  }

  def updateCenterDist(idx: Int): Unit = {
    val cent: Vector = lcCenters(idx)
    centerDist(idx) = cent.dot(cent)
  }

  def updateCenters(oldCenters: Array[Vector], oldVs: Vector, k: Int): Unit = {
    for (i <- 0 until k) {
      val centerUpdate = lcCenters(i).sub(oldCenters(i))
      centers.increment(centerUpdate)
    }
    val counterUpdate = lcV.sub(oldVs)
    v.increment(counterUpdate)
  }
}
