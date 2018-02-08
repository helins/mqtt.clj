(ns dvlopt.mqtt.v3.interop.clj

  ""

  {:author "Adam Helinski"}

  (:require [clojure.spec.alpha     :as s]
            [dvlopt.ex              :as ex]
            [dvlopt.mqtt            :as mqtt]
            [dvlopt.mqtt.v3.interop :as mqtt.v3.interop])
  (:import (org.eclipse.paho.client.mqttv3 MqttException
                                           MqttPersistenceException
                                           MqttMessage)))




;;;;;;;;;; Misc


(defmacro try-sync

  ""

  [& forms]

  `(try
     ~@forms
     (catch Throwable e#
       (exception e#))))




(defmacro try-async

  ""

  [cb & forms]

  `(try
     ~@forms
     (catch Throwable e#
       (when-some [cb'# ~cb]
         (cb'# (exception e#)
               nil)))))




;;;;;;;;;; Reason code


(s/fdef mqtt-exception--reason-code

  :args (s/cat :code ::mqtt/short)
  :ret  (s/nilable ::mqtt/blame))


(defn mqtt-exception--reason-code

  ""

  [code]

  (condp identical?
         code
    MqttException/REASON_CODE_BROKER_UNAVAILABLE           :broker-unavailable
    MqttException/REASON_CODE_CLIENT_ALREADY_DISCONNECTED  :already-disconnected
    MqttException/REASON_CODE_CLIENT_CLOSED                :closed
    MqttException/REASON_CODE_CLIENT_CONNECTED             :connected
    MqttException/REASON_CODE_CLIENT_DISCONNECT_PROHIBITED :disconnection-prohibited
    MqttException/REASON_CODE_CLIENT_DISCONNECTING         :disconnecting
    MqttException/REASON_CODE_CLIENT_EXCEPTION             :exception
    MqttException/REASON_CODE_CLIENT_NOT_CONNECTED         :not-connected
    MqttException/REASON_CODE_CLIENT_TIMEOUT               :response-timeout
    MqttException/REASON_CODE_CONNECT_IN_PROGRESS          :connection-in-progress
    MqttException/REASON_CODE_CONNECTION_LOST              :connection-lost
    MqttException/REASON_CODE_DISCONNECTED_BUFFER_FULL     :buffer-full
    MqttException/REASON_CODE_FAILED_AUTHENTICATION        :failed-authentication
    MqttException/REASON_CODE_INVALID_CLIENT_ID            :invalid-client-id
    MqttException/REASON_CODE_INVALID_MESSAGE              :invalid-message
    MqttException/REASON_CODE_INVALID_PROTOCOL_VERSION     :invalid-protocol-version
    MqttException/REASON_CODE_MAX_INFLIGHT                 :max-inflight
    MqttException/REASON_CODE_NO_MESSAGE_IDS_AVAILABLE     :no-msg-ids-available
    MqttException/REASON_CODE_NOT_AUTHORIZED               :not-authorized
    MqttException/REASON_CODE_SERVER_CONNECT_ERROR         :server-connection-error
    MqttException/REASON_CODE_SOCKET_FACTORY_MISMATCH      :socket-factory-mismatch
    MqttException/REASON_CODE_SSL_CONFIG_ERROR             :ssl-config-error
    MqttException/REASON_CODE_SUBSCRIBE_FAILED             :failed-subscription
    MqttException/REASON_CODE_TOKEN_INUSE                  :token-in-use
    MqttException/REASON_CODE_UNEXPECTED_ERROR             :unexpected
    MqttException/REASON_CODE_WRITE_TIMEOUT                :write-timeout
    nil))




(s/fdef mqtt-persistence-exception--reason-code

  :args (s/cat :code ::mqtt/short)
  :ret  (s/nilable ::mqtt/blame))


(defn mqtt-persistence-exception--reason-code

  ""

  [code]

  (if (identical? code
                  MqttPersistenceException/REASON_CODE_PERSISTENCE_IN_USE)
    :persistence-in-use
    (mqtt-exception--reason-code code)))




;;;;;;;;;; Exceptions


(s/fdef exception

  :args (s/cat :e ::ex/Throwable)
  :ret  ::mqtt/error)




(defprotocol IException

  ""

  (exception [e]

    ""))




(extend-protocol IException

  MqttException

    (exception [e]
      {::mqtt/blame     (mqtt-exception--reason-code (.getReasonCode e))
       ::mqtt/exception e})


  MqttPersistenceException

    (exception [e]
      {::mqtt/blame     (mqtt-persistence-exception--reason-code (.getReasonCode e))
       ::mqtt/exception e})


  Throwable

    (exception [e]
      {::mqtt/exception e}))




;;;;;;;;;; Paho classes


(s/fdef mqtt-message

  :args (s/cat :m ::mqtt.v3.interop/MqttMessage)
  :ret  ::mqtt/message.min)


(defn mqtt-message

  ""

  [^MqttMessage m]

  {::mqtt/message-id (.getId       m)
   ::mqtt/qos        (.getQos      m)
   ::mqtt/retained?  (.isRetained  m)
   ::mqtt/duplicate? (.isDuplicate m)
   ::mqtt/payload    (.getPayload  m)})
