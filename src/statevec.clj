(ns statevec
  ""
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.pprint :as pprint])
  (:import [org.opensky.libadsb ModeSDecoder]
           [org.opensky.libadsb.msgs ModeSReply]))

(set! *warn-on-reflection* true)

(def db-spec "jdbc:postgresql://wiseman@localhost:5432/orbital")


(defn reduce-results [rows]
  (let [got-first?_ (atom false)
        decoder (ModeSDecoder.)
        decode (fn [^bytes raw timestamp]
                 (.decode decoder raw))
        inc (fnil inc 0)]
    (->> rows
         (reduce (fn [state row]
                   (if (> (:num-rows state) 1000000)
                     (reduced state)
                     (let [timestamp (.getTime ^java.sql.Timestamp (:timestamp row))
                           data (:data row)
                           ^ModeSReply msg (decode data timestamp)]
                       (when (= (:num-rows state) 0)
                         (println "First record"))
                       (-> state
                           (update :num-rows inc)
                           (update-in [:msg-types (.getType msg)] inc)
                           (cond-> (:is_mlat row)
                             (update :num-mlats inc))
                           (cond-> (and (nil? (:mlat state)) (:is_mlat row))
                             (assoc :mlat [msg
                                           (.getType msg)
                                           (.getDownlinkFormat msg)
                                           (.getFirstField msg)
                                           (seq data)
                                           (org.opensky.libadsb.tools/toHexString data)]))
                           (cond-> (not (.checkParity msg))
                             (update :num-errors inc))
                           (cond-> (instance? org.opensky.libadsb.msgs.AirbornePositionV0Msg msg)
                             (update :num-position-reports inc))))))
                 {:num-rows 0
                  :num-errors 0
                  :num-position-reports 0
                  :num-mlats 0
                  :msg-types {}}))))


(defn test-query []
  (->> (jdbc/reducible-query
        db-spec
        "select * from pings where timestamp > '2018-11-28 00:00:00';")
       reduce-results))


(defn test-query2 []
  (jdbc/with-db-transaction [tx db-spec]
    (jdbc/query tx
                [(jdbc/prepare-statement (:connection tx)
                                         "select * from pings where timestamp > '2018-11-28 00:00:00';"
                                         {:fetch-size 1000})]
                {:result-set-fn (fn [result-set]
                                  (reduce-results result-set))})))


(defn -main [& args]
  (time (clojure.pprint/pprint (statevec/test-query2))))
