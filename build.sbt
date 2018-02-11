import Dependencies._

organization := "org.tomahna"
name := "blossom"
version      := "0.1.0-SNAPSHOT"
scalaVersion := "2.11.12"

libraryDependencies += magnolia
libraryDependencies += sparkCore
libraryDependencies += sparkSql
libraryDependencies += scalaTest % Test
