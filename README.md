# dvlopt.mqtt

[![Clojars
Project](https://img.shields.io/clojars/v/dvlopt/mqtt.svg)](https://clojars.org/dvlopt/mqtt)

This library is built updon the [Paho MQTT
Java](https://github.com/eclipse/paho.mqtt.java) client.

It provides clojure semantics and uses namespaced keywords for referring to
values and options. Those keywords are specified using clojure.spec.

## Usage

Read the [API](https://dvlopt.github.io/doc/clojure/dvlopt/mqtt/index.html).

Read the [longer
example](https://github.com/dvlopt/mqtt/blob/master/examples/dvlopt/mqtt/examples/v3/basic.clj).

In short :

```clj
(require '[dvlopt.mqtt    :as mqtt]
         '[dvlopt.mqtt.v3 :as mqtt.v3])


;; Here are the options we will use for our client.
;; We will connect to the public MQTT server provided by mosquitto.org

(def opts
     {::mqtt/nodes [{::mqtt/scheme :tcp
                     ::mqtt/host   "test.mosquitto.org"
                     ::mqtt/port   1883}]})


;; Now we build our client.

(def client
     (::mqtt.v3/client (mqtt.v3/open opts)))


;; The callback for the messages we receive from our example topic.

(defn on-message

  [message]

  (println "Received :" (String. ^bytes (::mqtt/payload message))))


;; We subscribe to our example topic.

(mqtt.v3/subscribe client
                   {"dvlopt/example/v3" {::mqtt/qos           1
                                         ::mqtt.v3/on-message on-message}})


;; And now we publish to this topic.

(mqtt.v3/publish client
                 "dvlopt/example/v3"
                 {:mqtt/payload (.getBytes "Hello Mqtt !")})
```

## License

Copyright © 2018 Adam Helinski

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
