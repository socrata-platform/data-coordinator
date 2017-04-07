package com.socrata.datacoordinator.secondary.messaging

trait MessageProducer {
  def start(): Unit
  def setServiceNames(serviceNames: Set[String]): Unit
  def shutdown(): Unit
  def send(message: Message): Unit
}

object NoOpMessageProducer extends MessageProducer {
  def start(): Unit = {}
  def setServiceNames(serviceNames: Set[String]): Unit = {}
  def shutdown(): Unit = {}
  def send(message: Message): Unit = {}
}
