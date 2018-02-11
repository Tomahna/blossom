package org.tomahna.blossom

import org.apache.spark.sql.types._
import org.scalatest.{FlatSpec, Matchers}
import org.tomahna.blossom.StructDerivation._

class StructBuilderSpec extends FlatSpec with Matchers{
  case class IntData(data: Int)
  "A StructBuilder" should "handle single int" in {
    struct[IntData]() shouldBe StructType(StructField("data", IntegerType, false) :: Nil)
  }
  case class LongData(data: Long)
  it should "handle single long" in {
    struct[LongData]() shouldBe StructType(StructField("data", LongType, false) :: Nil)
  }
  case class StringData(data: String)
  it should "handle single string" in {
    struct[StringData]() shouldBe StructType(StructField("data", StringType, false) :: Nil)
  }
  case class OptionIntData(data: Option[Int])
  it should "handle single optional int" in {
    struct[OptionIntData]() shouldBe StructType(StructField("data", IntegerType, true) :: Nil)
  }
  case class OptionLongData(data: Option[Long])
  it should "handle single optional long" in {
    struct[OptionLongData]() shouldBe StructType(StructField("data", LongType, true) :: Nil)
  }
  case class OptionStringData(data: Option[String])
  it should "handle single optional string" in {
    struct[OptionStringData]() shouldBe StructType(StructField("data", StringType, true) :: Nil)
  }
  case class ArrayIntData(data: Array[Int])
  it should "handle array of int" in {
    struct[ArrayIntData]() shouldBe StructType(StructField("data", ArrayType(IntegerType, false), false) :: Nil)
  }
  case class ArrayLongData(data: Array[Long])
  it should "handle array of long" in {
    struct[ArrayLongData]() shouldBe StructType(StructField("data", ArrayType(LongType, false), false) :: Nil)
  }
  case class ArrayStringData(data: Array[String])
  it should "handle array of string" in {
    struct[ArrayStringData]() shouldBe StructType(StructField("data", ArrayType(StringType, false), false) :: Nil)
  }
  case class ArrayOptionIntData(data: Array[Option[Int]])
  it should "handle array of optional int" in {
    struct[ArrayOptionIntData]() shouldBe StructType(StructField("data", ArrayType(IntegerType, true), false) :: Nil)
  }
  case class MultiArg(int: Int, long: Long, string: String, oInt: Option[Int], oLong: Option[Long], oString: Option[String])
  it should "handle multiple elements" in {
    struct[MultiArg]() shouldBe StructType(
      List(
        StructField("int", IntegerType, false),
        StructField("long", LongType, false),
        StructField("string", StringType, false),
        StructField("oInt", IntegerType, true),
        StructField("oLong", LongType, true),
        StructField("oString", StringType, true)
      )
    )
  }
  case class Inner(int: Int, long: Long, string: String)
  case class Outer(inner: Inner)
  it should "handle nested elements" in {
    struct[Outer]() shouldBe StructType(
      StructField("inner",
        StructType(
          List(
            StructField("int", IntegerType, false),
            StructField("long", LongType, false),
            StructField("string", StringType, false)
          )
        ),
        false
      ) :: Nil
    )
  }
  case class NestedL3(int: Int)
  case class NestedL2(nestedL3: NestedL3)
  case class NestedL1(nestedL2: NestedL2)
  case class NestedL0(nestedL1: NestedL1)
  it should "handle deeply nested elemnts" in {
    struct[NestedL0]() shouldBe StructType(
      StructField("nestedL1", StructType(
        StructField("nestedL2",  StructType(
          StructField("nestedL3", StructType(
            StructField("int", IntegerType, false) :: Nil
          ), false) :: Nil
        ), false) :: Nil
      ), false) :: Nil
    )
  }
}
