(ns dvlopt.mqtt

  ""

  {:author "Adam Helinski"}
  
  (:require [clojure.spec.alpha     :as s]
            [clojure.spec.gen.alpha :as gen]
            [dvlopt.ex              :as ex]
            [dvlopt.void            :as void])
  (:import javax.net.SocketFactory))




;;;;;;;;;; Declarations 


(declare uri)




;;;;;;;;;; Specs - Misc


(s/def ::int.pos

  (s/int-in 0
            (inc Integer/MAX_VALUE)))


(s/def ::long.pos

  (s/int-in 0
            Long/MAX_VALUE))


(s/def ::short

  (s/int-in Short/MIN_VALUE
            (inc Short/MAX_VALUE)))


(s/def ::string

  (s/and string?
         not-empty))




;;;;;;;;;; Specs - Errors


(s/def ::blame

  #{:broker-unavailable
    :already-disconnected
    :closed
    :connected
    :disconnection-prohibited
    :disconnecting
    :exception
    :not-connected
    :response-timeout
    :connection-in-progress
    :connection-lost
    :buffer-full
    :failed-authentication
    :invalid-client-id
    :invalid-message
    :invalid-protocol-version
    :max-inflight
    :no-msg-ids-available
    :not-authorized
    :persistence-in-use
    :server-connection-error
    :socket-factory-mismatch
    :ssl-config-error
    :failed-subscription
    :token-in-use
    :unexpected
    :write-timeout})


(s/def ::error

  (s/keys :req [::exception]
          :opt [::blame]))


(s/def ::exception

  ::ex/Throwable)




;;;;;;;;;; Specs - MQTT protocol and options


(s/def ::clean-session?

  boolean?)


(s/def ::client-id

  ::string)


(s/def ::connected?

  boolean?)


(s/def ::delay.msec.connection

  ::long.pos)


(s/def ::duplicate?

  boolean?)


(s/def ::host

  ::string)


(s/def ::interval.sec.keep-alive

  ::int.pos)


(s/def ::manual-acks?

  boolean?)


(s/def ::max-inflight

  ::int.pos)


(s/def ::message

  (s/merge ::message.min
           (s/keys :req [::topic])))


(s/def ::message.min

  (s/keys :req [::duplicate?
                ::message-id
                ::payload
                ::qos
                ::retained?]))


(s/def ::message-id

  ::int.pos)


(s/def ::node

  (s/keys :opt [::scheme
                ::host
                ::port]))


(s/def ::nodes

  (s/coll-of ::node))


(s/def ::password

  ::string)


(s/def ::payload

  bytes?)


(s/def ::port

  (s/int-in 0
            65536))


(s/def ::qos

  #{0 1 2})


(s/def ::retained?

  boolean?)


(s/def ::scheme

  #{:tcp
    :ssl})


(s/def ::socket-factory

  (s/with-gen #(instance? SocketFactory
                          %)
              (fn gen []
                (gen/fmap (fn socket [_]
                            (SocketFactory/getDefault))
                          (s/gen nil?)))))


(s/def ::timeout.msec.close

  ::long.pos)


(s/def ::timeout.sec.connect

  ::int.pos)


(s/def ::topic

  ::string)


(s/def ::topics

  (s/coll-of ::topic))


(s/def ::topic->qos

  (s/map-of ::topic
            ::qos))


(s/def ::username

  ::string)


(s/def ::uri

  (s/with-gen (s/and ::string
                     #(re-find #"^(?:tcp|ssl)://\w+:\d{0,5}$"
                               %))
              (fn gen []
                (gen/fmap (fn u [node]
                            (uri node))
                          (s/gen ::node)))))


(s/def ::uris

  (s/coll-of ::uri))


(s/def ::will.payload

  ::payload)


(s/def ::will.qos

  ::qos)


(s/def ::will.retained?

  ::retained?)


(s/def ::will.topic

  ::topic)




;;;;;;;;;; API - Defaults

;; TODO port

(let [host     "localhost"
      port-tcp 1883]

  (def defaults

    ""

    {::clean-session?          true
     ::host                    host
     ::interval.sec.keep-alive 60
     ::manual-acks?            false
     ::max-inflight            10
     ::nodes                   [{::scheme :tcp
                                 ::host   host
                                 ::port   port-tcp}]
     ::payload                 (byte-array 0)
     ::qos                     0
     ::retained?               false
     ::scheme                  :tcp
     ::timeout.msec.close      30000
     ::timeout.sec.connect     30
     ::will.qos                0
     ::will.retained?          true})


  (def defaults-port

    ""

    {:ssl 8883
     :tcp port-tcp}))





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

(s/fdef node->uri

  :args (s/cat :node (s/nilable ::node))
  :ret  ::uri)


(defn node->uri

  ""

  [node]

  (let [scheme  (void/obtain ::scheme
                             node
                             defaults)
        port    (or (::port node)
                    (get defaults-port
                         scheme))]
    (format (if port
              "%s://%s:%d"
              "%s://%s")
            (name scheme)
            (void/obtain ::host
                         node
                         defaults)
            port)))
