package com.socrata.datacoordinator.truth

import java.util.NoSuchElementException

import org.scalatest.FunSuite
import org.scalatest.matchers.MustMatchers
import org.scalatest.prop.PropertyChecks

class RowUserIdMapTest extends FunSuite with MustMatchers with PropertyChecks {

  test("Map begins empty") {
    val map = new SimpleRowUserIdMap
    map.isEmpty must be (true)
    map.size must be (0)
  }

  test("Put inserts into map") {
    forAll { (key: String, value: Int) =>
      val map = new SimpleRowUserIdMap[String, Int]

      map.put(key, value)

      map must have size (1)
      map.isEmpty must be (false)
      map.contains(key) must be (true)
      map.get(key) must be (Some(value))
      map(key) must be (value)
    }
  }

  test("Apply throws when no element") {
    val map = new SimpleRowUserIdMap[Int, String]

    forAll { key: Int =>
      val thrown = evaluating { map(key) } must produce [NoSuchElementException]
      thrown.getCause must be (null)
    }
  }

  test("Get returns None when absent, Some when present") {
    forAll { (key: Int, value: Int) =>
      val map = new SimpleRowUserIdMap[Int, Int]

      map.get(key) must be (None)
      map.put(key, value)
      map.get(key) must be (Some(value))
    }
  }

  test("Elements are removed from map") {
    val map = new SimpleRowUserIdMap[Int, Int]

    forAll { (key: Int, value: Int) =>
      map.put(key, value)
      map must have size (1)
      map.remove(key)
      map must have size (0)
    }
  }

  test("Clear removes all elements") {
    val map = new SimpleRowUserIdMap[Int, Int]

    forAll { (key: Int, value:Int) =>
      map.put(key, value)
    }

    map.clear()

    map must have size (0)
  }

  test("Size is accurate") {
    val map = new SimpleRowUserIdMap[Int, Int]

    forAll { (key: Int, value:Int) =>
      map.put(key, value)
    }

    map must have size (map.keysIterator.size)
    map must have size (map.valuesIterator.size)
  }

  test("Foreach iterates over each item exactly once") {
    val map = new SimpleRowUserIdMap[Int, Int]
    val map2 = new SimpleRowUserIdMap[Int, Int]

    forAll { (key: Int, value: Int) =>
      map.put(key, value)
      map2.put(key, value)
    }

    map.size must equal (map2.size)

    map.foreach { (k, v) =>
      map2.contains(k) must be (true)
      map2.remove(k)
    }

    map2 must have size (0)
  }
}
