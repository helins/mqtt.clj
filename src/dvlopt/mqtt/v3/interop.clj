(ns dvlopt.mqtt.v3.interop

  ""

  {:author "Adam Helinski"}

  (:require [clojure.spec.alpha     :as s]
            [clojure.spec.gen.alpha :as gen]
            [dvlopt.ex              :as ex]
            [dvlopt.mqtt            :as mqtt]
            [dvlopt.mqtt.v3.stores  :as mqtt.v3.stores]
            [dvlopt.void            :as void])
  (:import (org.eclipse.paho.client.mqttv3 DisconnectedBufferOptions
                                           IMqttActionListener
                                           IMqttAsyncClient
                                           IMqttMessageListener
                                           IMqttToken
                                           MqttAsyncClient
                                           MqttCallback
                                           MqttClientPersistence
                                           MqttConnectOptions
                                           MqttException
                                           MqttPersistenceException
                                           MqttMessage)
           org.eclipse.paho.client.mqttv3.persist.MemoryPersistence))




;;;;;;;;;; Declarations


(declare mqtt-exception--reason-code->clj
         mqtt-persistence-exception--reason-code->clj)




;;;;;;;;;; Specs - Clojure data structures


(s/def ::imqtt-token.connection

  (s/keys :req [::mqtt/uri
                ::mqtt/clean-session?]))


(s/def ::imqtt-token.delivery

  (s/keys :req [::mqtt/message-id
                ::mqtt/topic]))


(s/def ::imqtt-token.disconnection

  (s/keys :req [::mqtt/uri]))


(s/def ::imqtt-token.publication

  (s/keys :req [::mqtt/message-id
                ::mqtt/topic]))


(s/def ::imqtt-token.subscription

  (s/keys :req [::mqtt/message-id
                ::mqtt/topic->qos]))


(s/def ::imqtt-token.unsubscription

  (s/keys :req [::mqtt/message-id
                ::mqtt/topics]))



;;;;;;;;;; Specs - Paho classes


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

  ::mqtt.v3.stores/store)


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




;;;;;;;;;; Conversion - Misc


(defn string-array

  ""

  ^"[Ljava.lang.String;"

  [sq]

  (into-array String
              sq))




;;;;;;;;;; Conversion - Java classes to clojure data structures


(s/fdef exception->clj

  :args (s/cat :e ::ex/Throwable)
  :ret  ::mqtt/error)




(defprotocol MqttError

  ""

  (exception->clj [e]

    ""))




(extend-protocol MqttError

  MqttException

    (exception->clj [e]
      {::mqtt/blame     (mqtt-exception--reason-code->clj (.getReasonCode e))
       ::mqtt/exception e})


  MqttPersistenceException

    (exception->clj [e]
      {::mqtt/blame     (mqtt-persistence-exception--reason-code->clj (.getReasonCode e))
       ::mqtt/exception e})


  Throwable

    (exception->clj [e]
      {::mqtt/exception e}))




(s/fdef imqtt-token--topic

  :args (s/cat :token ::IMqttToken)
  :ret  ::mqtt/topic)


(defn imqtt-token--topic

  ""

  [^IMqttToken token]

  (first (.getTopics token)))




(s/fdef imqtt-token--uri

  :args (s/cat :token ::IMqttToken)
  :ret  ::mqtt/uri)


(defn imqtt-token--uri

  ""

  [^IMqttToken token]

  ;; TODO could throw ? cf. client API

  (.getCurrentServerURI ^MqttAsyncClient (.getClient token)))




(s/fdef imqtt-token->clj--connection

  :args (s/cat :token ::IMqttToken)
  :ret  ::imqtt-token.connection)


(defn imqtt-token->clj--connection

  ""

  [^IMqttToken token]

  {::mqtt/uri            (imqtt-token--uri token)
   ::mqtt/clean-session? (not (.getSessionPresent token))})




(s/fdef imqtt-token->clj--delivery

  :args (s/cat :token ::IMqttToken)
  :ret  ::imqtt-token.delivery)


(defn imqtt-token->clj--delivery

  ""

  [^IMqttToken token]

  {::mqtt/message-id (.getMessageId token)
   ::mqtt/topic      (imqtt-token--topic token)})




(s/fdef imqtt-token->clj--disconnection

  :args (s/cat :token ::IMqttToken)
  :ret  ::imqtt-token.disconnection)


(defn imqtt-token->clj--disconnection

  ""

  [^IMqttToken token]

  {::mqtt/uri (imqtt-token--uri token)})




(s/fdef imqtt-token->clj--publication

  :args (s/cat :token ::IMqttToken)
  :ret  ::imqtt-token.publication)


(defn imqtt-token->clj--publication

  ""

  [^IMqttToken token]

  {::mqtt/message-id (.getMessageId token)
   ::mqtt/topic      (imqtt-token--topic token)})




(s/fdef imqtt-token->clj--subscription

  :args (s/cat :token ::IMqttToken)
  :ret  ::imqtt-token.subscription)


(defn imqtt-token->clj--subscription

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




(s/fdef imqtt-token->clj--unsubscription

  :args (s/cat :token ::IMqttToken)
  :ret  ::imqtt-token.unsubscription)


(defn imqtt-token->clj--unsubscription

  ""

  [^IMqttToken token]

  {::mqtt/message-id (.getMessageId token)
   ::mqtt/topics     (seq (.getTopics token))})




(s/fdef mqtt-exception--reason-code->clj

  :args (s/cat :code ::mqtt/short)
  :ret  (s/nilable ::mqtt/blame))


(defn mqtt-exception--reason-code->clj

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




(s/fdef mqtt-persistence-exception--reason-code->clj

  :args (s/cat :code ::mqtt/short)
  :ret  (s/nilable ::mqtt/blame))


(defn mqtt-persistence-exception--reason-code->clj

  ""

  [code]

  (if (identical? code
                  MqttPersistenceException/REASON_CODE_PERSISTENCE_IN_USE)
    :persistence-in-use
    (mqtt-exception--reason-code->clj code)))




(s/fdef mqtt-message->clj

  :args (s/cat :m ::MqttMessage)
  :ret  ::mqtt/message.min)


(defn mqtt-message->clj

  ""

  [^MqttMessage m]

  {::mqtt/message-id (.getId       m)
   ::mqtt/qos        (.getQos      m)
   ::mqtt/retained?  (.isRetained  m)
   ::mqtt/duplicate? (.isDuplicate m)
   ::mqtt/payload    (.getPayload  m)})




;;;;;;;;;; Conversion - Clojure data structures to java classes


(s/fdef clj->disconnected-buffer-options

  :args (s/cat :opts (s/? (s/keys :opt [::mqtt.v3.stores/persisted?
                                        ::mqtt.v3.stores/size
                                        ::mqtt.v3.stores/when-full])))
  :ret  (s/keys :req [::DisconnectedBufferOptions
                      ::mqtt.v3.stores/size]
                :opt [::mqtt.v3.stores/persisted?
                      ::mqtt.v3.stores/when-full]))


(defn clj->disconnected-buffer-options

  ""

  (^DisconnectedBufferOptions

   []

   (clj->disconnected-buffer-options nil))


  (^DisconnectedBufferOptions

   [opts]

   (let [dbo  (DisconnectedBufferOptions.)
         size (void/obtain ::mqtt.v3.stores/size
                           opts
                           mqtt.v3.stores/defaults)]
     (merge {::DisconnectedBufferOptions dbo
             ::mqtt.v3.stores/size       size}
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




(s/fdef clj->imqtt-action-listener

  :args (s/cat :callback      (s/or :success (s/cat :error any?
                                                    :data  nil?)
                                    :failure (s/cat :error nil?
                                                    :data  any?))
               :map-exception (s/fspec :args (s/cat :exception ::ex/Throwable)
                                       :ret  any?)
               :map-token     (s/fspec :args (s/cat :token ::IMqttToken)
                                       :ret  any?))
  :ret ::IMqttActionListener)




(defn clj->imqtt-action-listener

  ""

  ;; TODO spec ?

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




(s/fdef clj->imqtt-message-listener

  :args (s/cat :callback (s/fspec :args (s/cat :message ::mqtt/message)
                                  :ret  nil?))
  :ret ::IMqttMessageListener)


(defn clj->imqtt-message-listener

  ""

  ^IMqttMessageListener

  [callback]

  (reify

    IMqttMessageListener

      (messageArrived [_ topic message]
        (callback (assoc (mqtt-message->clj message)
                         ::mqtt/topic
                         topic)))))




(s/fdef clj->mqtt-callback

  :args (s/cat :on-connection-lost   (s/fspec :args (s/cat :error ::mqtt/error)
                                              :ret  nil?)
               :on-message-delivered (s/fspec :args (s/cat :data ::imqtt-token.delivery)
                                              :ret  nil?)
               :on-message-received  (s/fspec :args (s/cat :message ::mqtt/message)
                                              :ret  nil?))
  :ret ::MqttCallback)


(defn clj->mqtt-callback

  ""

  ^MqttCallback

  [on-connection-lost on-message-delivered on-message-received]

  (reify

    MqttCallback

      (connectionLost [_ exception]
        (void/call on-connection-lost
                   (exception->clj exception))
        nil)


      (deliveryComplete [_ token]
        (void/call on-message-delivered
                   (imqtt-token->clj--delivery token))
        nil)


      (messageArrived [_ topic msg]
        (void/call on-message-received
                   (assoc (mqtt-message->clj msg)
                          ::mqtt/topic
                          topic))
        nil)))




(s/fdef clj->mqtt-connect-options

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
                      ::MqttConnectOptions]
                :opt [::mqtt/will.qos
                      ::mqtt/will.retained?]))


(defn clj->mqtt-connect-options

  ""

  (^MqttConnectOptions

   []

   (clj->mqtt-connect-options nil))


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
             ::mqtt/uris                          (let [uris (map mqtt/node->uri
                                                                  (void/obtain ::mqtt/nodes
                                                                               opts
                                                                               mqtt/defaults))]
                                                    (.setServerURIs mco
                                                                    (string-array uris))
                                                    uris)
             ::MqttConnectOptions mco}
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
