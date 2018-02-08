(ns dvlopt.mqtt.v3.interop

  ""

  {:author "Adam Helinski"}

  (:require [clojure.spec.alpha     :as s]
            [clojure.spec.gen.alpha :as gen])
  (:import (org.eclipse.paho.client.mqttv3 DisconnectedBufferOptions
                                           IMqttActionListener
                                           IMqttAsyncClient
                                           IMqttMessageListener
                                           IMqttToken
                                           MqttCallback
                                           MqttClientPersistence
                                           MqttConnectOptions
                                           MqttMessage)
           org.eclipse.paho.client.mqttv3.persist.MemoryPersistence))




;;;;;;;;;;


(s/def ::DisconnectedBufferOptions

  (s/with-gen #(instance? DisconnectedBufferOptions
                          %)
              (fn gen []
                (gen/fmap (fn dbo [_]
                            (DisconnectedBufferOptions.))
                          (s/gen nil?)))))


(s/def ::IMqttActionListener

  #(instance? IMqttActionListener))


(s/def ::IMqttAsyncClient

  #(instance? IMqttAsyncClient
              %))


(s/def ::IMqttMessageListener

  #(instance? IMqttMessageListener
              %))


(s/def ::IMqttToken

  #(instance? IMqttToken
              %))


(s/def ::MqttCallback

  #(instance? MqttCallback
              %))


(s/def ::MqttClientPersistence

  (s/with-gen #(instance? MqttClientPersistence
                          %)
              (fn gen []
                (gen/fmap (fn store [_]
                            (MemoryPersistence.))
                          (s/gen nil?)))))


(s/def ::MqttConnectOptions

  (s/with-gen #(instance? MqttConnectOptions
                          %)
              (fn gen []
                (gen/fmap (fn mco [_]
                            (MqttConnectOptions.))
                          (s/gen nil?)))))


(s/def ::MqttMessage

  (s/with-gen #(instance? MqttMessage
                          %)
              (fn gen []
                (gen/fmap (fn mqtt-message [^bytes ba]
                            (MqttMessage. ba))
                          (s/gen bytes?)))))
