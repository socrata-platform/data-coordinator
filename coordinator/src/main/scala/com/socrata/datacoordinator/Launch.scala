package com.socrata.datacoordinator

object Launch extends App {
  if(args.length > 0) {
    val className = args(0)
    val subargs = args.drop(1)

    val cls = Class.forName(className)
    cls.getMethod("main", classOf[Array[String]]).invoke(null, subargs)
  } else {
    Console.err.println("Usage: Launch CLASSNAME [ARGS...]")
    sys.exit(1)
  }
}
