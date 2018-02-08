(ns dvlopt.mqtt.v3.stores

  ""

  {:author "Adam Helinski"}

  (:require [clojure.spec.alpha     :as s]
            [clojure.spec.gen.alpha :as gen]
            [dvlopt.mqtt            :as mqtt]
            [dvlopt.mqtt.v3.interop :as mqtt.v3.interop])
  (:import (org.eclipse.paho.client.mqttv3.persist MemoryPersistence
                                                   MqttDefaultFilePersistence)))




;;;;;;;;;; Specs


(s/def ::store

  ::mqtt.v3.interop/MqttClientPersistence)


(s/def ::size

  (s/int-in 0
            Integer/MAX_VALUE))


(s/def ::persisted?

  boolean?)


(s/def ::when-full

  #{:throw
    :delete-oldest})




;;;;;;;;;; API - Defaults


(def defaults

  ""

  {::size       5000
   ::persisted? false
   ::when-full  :throw})




;;;;;;;;;; API - Building stores


(s/fdef flat-files

  :args (s/cat :path (s/? string?))
  :ret  ::store)


(defn flat-files

  ;; TODO Drops qos 0 messages across restarts

  ""

  ^MqttDefaultFilePersistence

  ([]

   (flat-files (System/getProperty "user.dir")))


  ([path]

   (MqttDefaultFilePersistence. path)))




(s/fdef ram

  :args nil?
  :ret  ::store)


(defn ram

  ""

  ^MemoryPersistence

  []

  (MemoryPersistence.))
