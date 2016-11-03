package com.socrata.datacoordinator.secondary.messaging

trait Producer {
  def start(): Unit
  def setServiceNames(serviceNames: Set[String]): Unit
  def shutdown(): Unit
  def send(message: Message): Unit
}

object NoOpProducer extends Producer {
  def start(): Unit = {}
  def setServiceNames(serviceNames: Set[String]): Unit = {}
  def shutdown(): Unit = {}
  def send(message: Message): Unit = {}
}
