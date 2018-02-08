(ns dvlopt.mqtt.v3.interop.java

  ""

  {:author "Adam Helinski"}

  (:require [clojure.spec.alpha                :as s]
            [dvlopt.ex                         :as ex]
            [dvlopt.mqtt                       :as mqtt]
            [dvlopt.mqtt.v3.interop            :as mqtt.v3.interop]
            [dvlopt.mqtt.v3.interop.clj        :as mqtt.v3.interop.clj]
            [dvlopt.mqtt.v3.interop.clj.tokens :as mqtt.v3.interop.clj.tokens]
            [dvlopt.mqtt.v3.stores             :as mqtt.v3.stores]
            [dvlopt.void                       :as void])
  (:import (org.eclipse.paho.client.mqttv3 DisconnectedBufferOptions
                                           IMqttActionListener
                                           IMqttMessageListener
                                           MqttCallback
                                           MqttConnectOptions)))




;;;;;;;;;; Misc


(defn string-array

  ""

  ^"[Ljava.lang.String;"

  [sq]

  (into-array String
              sq))




;;;;;;;;;; Paho classes


(s/fdef disconnected-buffer-options

  :args (s/cat :opts (s/? (s/keys :opt [::mqtt.v3.stores/persisted?
                                        ::mqtt.v3.stores/size
                                        ::mqtt.v3.stores/when-full])))
  :ret  (s/keys :req [::mqtt.v3.interop/DisconnectedBufferOptions
                      ::mqtt.v3.stores/size]
                :opt [::mqtt.v3.stores/persisted?
                      ::mqtt.v3.stores/when-full]))


(defn disconnected-buffer-options

  ""

  (^DisconnectedBufferOptions

   []

   (disconnected-buffer-options nil))


  (^DisconnectedBufferOptions

   [opts]

   (let [dbo  (DisconnectedBufferOptions.)
         size (void/obtain ::mqtt.v3.stores/size
                           opts
                           mqtt.v3.stores/defaults)]
     (merge {::mqtt.v3.interop/DisconnectedBufferOptions dbo
             ::mqtt.v3.stores/size                       size}
            (if (pos? size)
              (let [persisted? (void/obtain ::mqtt.v3.stores/persisted?
                                            opts
                                            mqtt.v3.stores/defaults)
                    when-full  (void/obtain ::mqtt.v3.stores/when-full
                                            opts
                                            mqtt.v3.stores/defaults)]
                (doto dbo
                  (.setBufferEnabled        true)
                  (.setBufferSize           size)
                  (.setPersistBuffer        persisted?)
                  (.setDeleteOldestMessages (condp identical?
                                                   when-full
                                              :throw         false
                                              :delete-oldest true)))
                {::mqtt.v3.stores/persisted? persisted?
                 ::mqtt.v3.stores/when-full  when-full})
              (do
                (doto dbo
                  (.setBufferEnabled false)
                  (.setBufferSize    1)
                  (.setPersistBuffer false))
                nil))))))




(s/fdef imqtt-action-listener

  :args (s/cat :callback      (s/or :success (s/cat :error any?
                                                    :data  nil?)
                                    :failure (s/cat :error nil?
                                                    :data  any?))
               :map-exception (s/fspec :args (s/cat :exception ::ex/Throwable)
                                       :ret  any?)
               :map-token     (s/fspec :args (s/cat :token ::mqtt.v3.interop/IMqttToken)
                                       :ret  any?))
  :ret ::mqtt.v3.interop/IMqttActionListener)


(defn imqtt-action-listener

  ""

  ^IMqttActionListener

  [callback map-exception map-token]

  (reify

    IMqttActionListener

      (onFailure [_ _token exception]
        (callback (map-exception exception)
                  nil))


      (onSuccess [_ token]
        (callback nil
                  (map-token token)))))




(s/fdef imqtt-message-listener

  :args (s/cat :callback (s/fspec :args (s/cat :message ::mqtt/message)
                                  :ret  nil?))
  :ret ::mqtt.v3.interop/IMqttMessageListener)


(defn imqtt-message-listener

  ""

  ^IMqttMessageListener

  [callback]

  (reify

    IMqttMessageListener

      (messageArrived [_ topic message]
        (callback (assoc (mqtt.v3.interop.clj/mqtt-message message)
                         ::mqtt/topic
                         topic)))))




(s/fdef mqtt-callback

  :args (s/cat :on-connection-lost   (s/fspec :args (s/cat :error ::mqtt/error)
                                              :ret  nil?)
               :on-message-delivered (s/fspec :args (s/cat :data ::mqtt.v3.interop.clj.tokens/delivery)
                                              :ret  nil?)
               :on-message-received  (s/fspec :args (s/cat :message ::mqtt/message)
                                              :ret  nil?))
  :ret ::mqtt.v3.interop/MqttCallback)


(defn mqtt-callback

  ""

  ^MqttCallback

  [on-connection-lost on-message-delivered on-message-received]

  (reify

    MqttCallback

      (connectionLost [_ exception]
        (void/call on-connection-lost
                   (mqtt.v3.interop.clj/exception exception))
        nil)


      (deliveryComplete [_ token]
        (void/call on-message-delivered
                   (mqtt.v3.interop.clj.tokens/delivery token))
        nil)


      (messageArrived [_ topic msg]
        (void/call on-message-received
                   (assoc (mqtt.v3.interop.clj/mqtt-message msg)
                          ::mqtt/topic
                          topic))
        nil)))




(s/fdef mqtt-connect-options

  :args (s/cat :opts (s/? (s/keys :opt [::mqtt/clean-session?
                                        ::mqtt/max-inflight
                                        ::mqtt/nodes
                                        ::mqtt/password
                                        ::mqtt/socket-factory
                                        ::mqtt/timeout.sec.connect
                                        ::mqtt/interval.sec.keep-alive
                                        ::mqtt/username
                                        ::mqtt/will.payload
                                        ::mqtt/will.qos
                                        ::mqtt/will.retained?
                                        ::mqtt/will.topic])))
  :ret  (s/keys :req [::mqtt/clean-session?
                      ::mqtt/interval.sec.keep-alive
                      ::mqtt/max-inflight
                      ::mqtt/timeout.sec.connect
                      ::mqtt/uris
                      ::mqtt.v3.interop/MqttConnectOptions]
                :opt [::mqtt/will.qos
                      ::mqtt/will.retained?]))


(defn mqtt-connect-options

  ""

  (^MqttConnectOptions

   []

   (mqtt-connect-options nil))


  (^MqttConnectOptions

   [opts]

   (let [mco (MqttConnectOptions.)]
     (.setAutomaticReconnect mco
                             false)
     (when-some [^String password (::mqtt/password opts)]
       (.setPassword mco
                     (.toCharArray password)))
     (some->> (::mqtt/socket-factory opts)
              (.setSocketFactory mco))
     (some->> (::mqtt/username opts)
              (.setUserName mco))
     (merge {::mqtt/clean-session?                (let [? (void/obtain ::mqtt/clean-session?
                                                                       opts
                                                                       mqtt/defaults)]
                                                    (.setCleanSession mco
                                                                      ?)
                                                    ?)
             ::mqtt/interval.sec.keep-alive       (let [interval (void/obtain ::mqtt/interval.sec.keep-alive
                                                                              opts
                                                                              mqtt/defaults)]
                                                    (.setKeepAliveInterval mco
                                                                           interval)
                                                    interval)
             ::mqtt/max-inflight                  (let [n (void/obtain ::mqtt/max-inflight
                                                                       opts
                                                                       mqtt/defaults)]
                                                    (.setMaxInflight mco
                                                                     n)
                                                    n)
             ::mqtt/timeout.sec.connect           (let [timeout (void/obtain ::mqtt/timeout.sec.connect
                                                                             opts
                                                                             mqtt/defaults)]
                                                    (.setConnectionTimeout mco
                                                                           timeout)
                                                    timeout)
             ::mqtt/uris                          (let [uris (map mqtt/uri
                                                                  (void/obtain ::mqtt/nodes
                                                                               opts
                                                                               mqtt/defaults))]
                                                    (.setServerURIs mco
                                                                    (string-array uris))
                                                    uris)
             ::mqtt.v3.interop/MqttConnectOptions mco}
            (let [^bytes  payload (::mqtt/will.payload opts)
                  ^String topic   (::mqtt/will.topic   opts)]
              (when (and payload
                         topic)
                (let [^int     qos       (void/obtain ::mqtt/will.qos
                                                      opts
                                                      mqtt/defaults)
                      ^boolean retained? (void/obtain ::mqtt/will.retained?
                                                      opts
                                                      mqtt/defaults)]
                  (.setWill mco
                            topic
                            payload
                            qos
                            retained?)
                  {::mqtt/will.qos       qos
                   ::mqtt/will.retained? retained?})))))))
