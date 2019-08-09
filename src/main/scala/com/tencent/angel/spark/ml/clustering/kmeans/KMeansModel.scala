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

  def train(trainData: RDD[LabeledData], param: Param): Unit = {
    // Number of samples for each cluster
    val spCountPerCenter = new Array[Int](K)

    initKCentersRandomly(trainData)

    for (epoch <- 1 to param.numEpoch) {
      val startEpoch = System.currentTimeMillis()
      trainOneEpoch(epoch, trainData, spCountPerCenter)
      val epochTime = System.currentTimeMillis() - startEpoch

      val startObj = System.currentTimeMillis()
      val localObj = computeObjValue(trainData, epoch)
      val objTime = System.currentTimeMillis() - startObj
    }
  }

  def initKCentersRandomly(trainData: RDD[LabeledData]): Unit = {
    trainData.foreachPartition(iter => {
      val partId = TaskContext.getPartitionId
      if (partId == 0) {
      }
    })
  }

  /**
    * Each epoch updation is performed in three steps. First, pull the centers updated by last
    * epoch from PS. Second, a mini batch of size batch_sample_num is sampled. Third, update the
    * centers in a mini-batch way.
    *
    * @param trainData              : the trainning data storage
    * @param per_center_step_counts : the array caches the number of samples per center
    */
  def trainOneEpoch(epoch: Int, trainData: RDD[LabeledData], per_center_step_counts: Array[Int]): Unit = {

  }

  /**
    * Compute the objective values of all samples, which is measured by the distance from a
    * sample to its closest center.
    *
    * @param data : the trainning dataset
    * @param epoch       : the epoch number
    */
  def computeObjValue(data: RDD[LabeledData], epoch: Int): Double = {
    var obj = 0.0
    obj
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
