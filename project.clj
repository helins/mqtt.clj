(defproject dvlopt/mqtt
            "0.0.0"

  :description  "Async MQTT 3.x client"
  :url          "https://github.com/dvlopt/qoqos"
  :license      {:name "Eclipse Public License"
                 :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[dvlopt/void                                     "0.0.0"]
                 [dvlopt/ex                                       "1.0.0"]
                 [org.eclipse.paho/org.eclipse.paho.client.mqttv3 "1.2.0"]]
  :profiles     {:dev {:source-paths ["dev"]
                       :main         user
                       :dependencies [[criterium              "0.4.4"]
                                      [org.clojure/clojure    "1.9.0"]
                                      [org.clojure/test.check "0.10.0-alpha2"]]
                       :plugins      [[lein-codox      "0.10.3"]
                                      [venantius/ultra "0.5.2"]]
                       :codox        {:output-path  "doc/auto"
                                      :source-paths ["src"]}
                       :global-vars  {*warn-on-reflection* true}}})
