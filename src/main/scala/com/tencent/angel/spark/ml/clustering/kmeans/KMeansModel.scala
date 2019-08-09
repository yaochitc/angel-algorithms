package com.tencent.angel.spark.ml.clustering.kmeans

import java.util

import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.{IntFloatVector, Vector}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD

object KMeansModel {
  val KMEANS_CENTERS_MAT = "kmeans_centers"
  val KMEANS_V_MAT = "kmeans_v"
  val KMEANS_OBJ = "kmeans_obj"
}


class KMeansModel(ks: Array[Int]) extends Serializable {
  private val K = ks.product

  var lcCenters: util.List[Vector] = new util.ArrayList[Vector]()
  var lcV: IntFloatVector = VFactory.denseFloatVector(K)
  val centerDist = new Array[Double](K)

  def train(param: Param): Unit = {
  }

  def initKCentersRandomly(data: RDD[LabeledData], index: Int): Unit = {
    data.foreachPartition(iter => {
      val partId = TaskContext.getPartitionId
      if (partId == 0) {
      }
    })
  }

  /**
    * Pull centers from PS to local worker
    */
  def pullCentersFromPS(): Unit = {
  }

  /**
    * Pull mini-batch samples from PS to local worker
    */
  def pullVFromPS(): Unit = {
  }

  /**
    * Calculate the distance between instance and centers, and find the closest center
    *
    * @param x : a instance
    * @return : the closest center id
    */
  def findClosestCenter(x: Vector): (Int, Double) = {
    var minDis = Double.MaxValue
    var clstCent: Int = -1

    val len2 = x.dot(x)
    for (i <- 0 until K) {
      val dist = centerDist(i) - 2 * lcCenters.get(i).dot(x) + len2
      if (dist < minDis) {
        minDis = dist
        clstCent = i
      }
    }
    (clstCent, Math.sqrt(minDis))
  }

  def updateCenterDist(idx: Int): Unit = {
    val cent: Vector = lcCenters.get(idx)
    centerDist(idx) = cent.dot(cent)
  }
}
