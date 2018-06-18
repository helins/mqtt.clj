(ns dvlopt.mqtt.v3.stores

  "Stores buffer qos 1 and 2 messages for reliability."

  {:author "Adam Helinski"}

  (:require [clojure.spec.alpha     :as s]
            [clojure.spec.gen.alpha :as gen]
            [dvlopt.mqtt            :as mqtt])
  (:import org.eclipse.paho.client.mqttv3.MqttClientPersistence
           (org.eclipse.paho.client.mqttv3.persist MemoryPersistence
                                                   MqttDefaultFilePersistence)))




;;;;;;;;;; Specs


(s/def ::store

  (s/with-gen #(instance? MqttClientPersistence
                            %)
                (fn gen []
                  (gen/fmap (fn store [_]
                              (MemoryPersistence.))
                            (s/gen nil?)))))


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

  "Defaults values for specified keywords related to stores."

  {::size       5000
   ::persisted? false
   ::when-full  :throw})




;;;;;;;;;; API - Building stores


(s/fdef flat-files

  :args (s/cat :path (s/? string?))
  :ret  ::store)


(defn flat-files

  ;; TODO Drops qos 0 messages across restarts

  "Persists messages to flat files in the given directory (current directory by default).
  
   a sub-directory is created for each client-id and connection uri."

  (^MqttDefaultFilePersistence
   
   []

   (flat-files (System/getProperty "user.dir")))


  (^MqttDefaultFilePersistence

   [path]

   (MqttDefaultFilePersistence. path)))




(s/fdef ram

  :args nil?
  :ret  ::store)


(defn ram

  "Holds messages in ram, hence will not survive restarts."

  ^MemoryPersistence

  []

  (MemoryPersistence.))
