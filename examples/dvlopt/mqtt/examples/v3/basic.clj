(ns dvlopt.mqtt.examples.v3.basic

  "A basic example.
  
   We will use the public MQTT server provided by mosquitto.org."

  {:author "Adam Helinski"}

  (:require [dvlopt.mqtt    :as mqtt]
            [dvlopt.mqtt.v3 :as mqtt.v3]))




;;;;;;;;;;


(def opts

  "Options for building a client."

  {::mqtt/nodes                   [{::mqtt/scheme :tcp
                                    ::mqtt/host   "test.mosquitto.org"
                                    ::mqtt/port   1883}]
   ::mqtt/clean-session?          true
   ::mqtt.v3/on-connection        (fn [error data]
                                    (if error
                                      (println "<!> Unable to connect the client :" error)
                                      (println "Client connected :" data)))
   ::mqtt.v3/on-connection-lost   (fn [error]
                                    (println "Connection lost, reconnection attempt in 2 second.")
                                    ((::mqtt.v3/connect error) 2000))
   ::mqtt.v3/on-message-delivered (fn [data]
                                    (println "Message delivered :" data))
   ::mqtt.v3/on-message-unhandled (fn [message]
                                    (println "Message received for a topic without handler :" message))})




(defn subscribe

  "Subscribes the given client to our example topic."

  [client]

  (mqtt.v3/subscribe client
                     {"some/topic" {::mqtt/qos           2
                                    ::mqtt.v3/on-message (fn [message]
                                                           (println "Received :" (update message
                                                                                         ::mqtt/payload
                                                                                         (fn [^bytes ba]
                                                                                           (String. ba)))))}}
                     {::on-subscribed (fn [error data]
                                        (if error
                                          (println "Error during subscription :" error)
                                          (println "Subscription succeeded :" data)))}))




(defn publish

  "Publishes a string message to our example topic."

  [client ^String message]

  (mqtt.v3/publish client
                   "some/topic"
                   {::mqtt/payload         (.getBytes message)
                    ::mqtt/qos             2
                    ::mqtt.v3/on-published (fn [error data]
                                             (if error
                                               (println "Error during publication :" error)
                                               (println "Publication succeeded :" data)))}))




(comment


  (def client

    "MQTT client we build using our options."

    (::mqtt.v3/client (mqtt.v3/open opts)))


  )
