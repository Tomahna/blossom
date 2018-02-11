package org.tomahna.blossom

import org.apache.spark.sql.types._
import language.experimental.macros
import magnolia._

trait StructBuilder[T] {
  def build(): StructField
}

object StructDerivation {
  implicit val intField: StructBuilder[Int] = new StructBuilder[Int]{
    def build(): StructField = StructField("", IntegerType, false)
  }
  implicit val longField: StructBuilder[Long] = new StructBuilder[Long]{
    def build(): StructField = StructField("", LongType, false)
  }
  implicit val stringField: StructBuilder[String] = new StructBuilder[String]{
    def build(): StructField = StructField("", StringType, false)
  }
  implicit def optionField[T](implicit builder: StructBuilder[T]): StructBuilder[Option[T]] = new StructBuilder[Option[T]]{
    def build(): StructField = builder.build().copy(nullable = true)
  }

  type Typeclass[T] = StructBuilder[T]

  def combine[T](ctx: CaseClass[StructBuilder, T]): StructBuilder[T] = new StructBuilder[T] {
    def build(): StructField = {
      val fields = ctx.parameters.map { p => p.typeclass.build().copy(name= p.label) }
      StructField("", StructType(fields), false)
    }
  }

  def dispatch[T](ctx: SealedTrait[StructBuilder, T]): StructBuilder[T] =
    new StructBuilder[T] {
      def build(): StructField = ???
    }

  implicit def gen[T]: StructBuilder[T] = macro Magnolia.gen[T]

  def struct[T]()(implicit builder: StructBuilder[T]): StructType =
    builder.build().dataType.asInstanceOf[StructType]
}
