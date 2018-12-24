package spark

import java.io.Serializable

import scala.math.Ordered

/**
  * 自定义排序：
  * 进行排序需要几个字段：点击次数、下单次数、支付次数
  * 需要实现Ordered接口中的几个方法进行排序
  * 需要与其它几个key进行比较，判断大于、大于等于、小于、小于等于
  * 必须要实现Serializable接口进行序列化
  */
class CategorySortKey(// 点击次数
                      var clickCount: Long, // 下单次数
                      var orderCount: Long, // 支付次数
                      var payCount: Long) extends Ordered[CategorySortKey] with Serializable {
  override def compare(that: CategorySortKey): Int = {
    if (clickCount - that.getClickCount != 0) return (clickCount - that.getClickCount).toInt
    else if (orderCount - that.getOrderCount != 0) return (orderCount - that.getOrderCount).toInt
    else if (payCount - that.getPayCount != 0) return (payCount - that.getPayCount).toInt
    0
  }

  override def $less(that: CategorySortKey): Boolean = {
    if (clickCount < that.getClickCount) return true
    else if (clickCount == that.getClickCount
      && orderCount < that.getOrderCount) return true
    else if (clickCount == that.getClickCount
      && orderCount == that.getOrderCount && payCount < that.getPayCount) return true
    false
  }

  override def $greater(that: CategorySortKey): Boolean = {
    if (clickCount > that.getClickCount) return true
    else if (clickCount == that.getClickCount
      && orderCount > that.getOrderCount) return true
    else if (clickCount == that.getClickCount
      && orderCount == that.getOrderCount && payCount > that.getPayCount) return true
    false
  }

  override def $less$eq(that: CategorySortKey): Boolean = {
    if ($less(that)) return true
    else if (clickCount == that.getClickCount
      && orderCount == that.getOrderCount && payCount == that.getPayCount) return true
    false
  }

  override def $greater$eq(that: CategorySortKey): Boolean = {
    if ($greater(that)) return true
    else if (clickCount == that.getClickCount
      && orderCount == that.getOrderCount && payCount == that.getPayCount) return true
    false
  }

  override def compareTo(that: CategorySortKey): Int = compare(that)

  def getClickCount: Long = clickCount

  def setClickCount(clickCount: Long): Unit = this.clickCount = clickCount

  def getOrderCount: Long = orderCount

  def setOrderCount(orderCount: Long): Unit = this.orderCount = orderCount

  def getPayCount: Long = payCount

  def setPayCount(payCount: Long): Unit = this.payCount = payCount
}