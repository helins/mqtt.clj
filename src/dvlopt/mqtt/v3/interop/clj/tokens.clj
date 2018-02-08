(ns dvlopt.mqtt.v3.interop.clj.tokens

  ""

  {:author "Adam Helinski"}

  (:require [clojure.spec.alpha     :as s]
            [dvlopt.mqtt            :as mqtt]
            [dvlopt.mqtt.v3.interop :as mqtt.v3.interop])
  (:import (org.eclipse.paho.client.mqttv3 IMqttToken
                                           MqttAsyncClient)))




;;;;;;;;;; Specs


(s/def ::connection

  (s/keys :req [::mqtt/uri
                ::mqtt/clean-session?]))


(s/def ::delivery

  (s/keys :req [::mqtt/message-id
                ::mqtt/topic]))


(s/def ::disconnection

  (s/keys :req [::mqtt/uri]))


(s/def ::publication

  (s/keys :req [::mqtt/message-id
                ::mqtt/topic]))


(s/def ::subscription

  (s/keys :req [::mqtt/message-id
                ::mqtt/topic->qos]))


(s/def ::unsubscription

  (s/keys :req [::mqtt/message-id
                ::mqtt/topics]))




;;;;;;;;;; Private


(s/fdef -uri

  :args (s/cat :token ::mqtt.v3.interop/IMqttToken)
  :ret  ::mqtt/uri)


(defn- -uri

  ""

  [^IMqttToken token]

  ;; TODO could throw ? cf. client API

  (.getCurrentServerURI ^MqttAsyncClient (.getClient token)))




(s/fdef -topic

  :args (s/cat :token ::mqtt.v3.interop/IMqttToken)
  :ret  ::mqtt/topic)


(defn- -topic

  ""

  [^IMqttToken token]

  (first (.getTopics token)))




;;;;;;;;;; Public


(s/fdef connection

  :args (s/cat :token ::mqtt.v3.interop/IMqttToken)
  :ret  ::connection)


(defn connection

  ""

  [^IMqttToken token]

  {::mqtt/uri            (-uri token)
   ::mqtt/clean-session? (not (.getSessionPresent token))})




(s/fdef delivery

  :args (s/cat :token :IMqttToken)
  :ret  ::delivery)


(defn delivery

  ""

  [^IMqttToken token]

  {::mqtt/message-id (.getMessageId token)
   ::mqtt/topic      (-topic token)})




(s/fdef disconnection

  :args (s/cat :token ::mqtt.v3.interop/IMqttToken)
  :ret  ::disconnection)


(defn disconnection

  ""

  [^IMqttToken token]

  {::mqtt/uri (-uri token)})




(s/fdef publication

  :args (s/cat :token ::mqtt.v3.interop/IMqttToken)
  :ret  ::publication)


(defn publication

  ""

  [^IMqttToken token]

  {::mqtt/message-id (.getMessageId token)
   ::mqtt/topic      (-topic token)})




(s/fdef subscription

  :args (s/cat :token ::mqtt.v3.interop/IMqttToken)
  :ret  ::subscription)


(defn subscription

  ""

  [^IMqttToken token]

  {::mqtt/message-id (.getMessageId token)
   ::mqtt/topic->qos (reduce (fn assemble [topic->qos [topic qos]]
                               (assoc topic->qos
                                      topic
                                      qos))
                             {}
                             (partition 2
                                        (interleave (.getTopics     token)
                                                    (.getGrantedQos token))))})




(s/fdef unsubscription

  :args (s/cat :token ::mqtt.v3.interop/IMqttToken)
  :ret  ::unsubscription)


(defn unsubscription

  ""

  [^IMqttToken token]

  {::mqtt/message-id (.getMessageId token)
   ::mqtt/topics     (seq (.getTopics token))})
