(ns statevec
  ""
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.pprint :as pprint]
            [clojure.string :as string]
            [java-time :as jt])
  (:import [java.time.format DateTimeFormatter]
           [org.opensky.libadsb
            ModeSDecoder
            Position]
           [org.opensky.libadsb.msgs
            AirbornePositionV0Msg
            EmergencyOrPriorityStatusMsg
            ModeSReply
            SurfacePositionV0Msg
            TCASResolutionAdvisoryMsg]))

(set! *warn-on-reflection* true)

(def db-spec "jdbc:postgresql://wiseman@localhost:5432/orbital")


(defn log
  ([msg]
   (println (jt/format DateTimeFormatter/ISO_INSTANT (jt/instant)) msg))
  ([fmt & args]
   (log (apply format fmt args))))


;; (defn reduce-results [rows]
;;   (let [got-first?_ (atom false)
;;         decoder (ModeSDecoder.)
;;         decode (fn [^bytes raw timestamp]
;;                  (.decode decoder raw))
;;         inc (fnil inc 0)]
;;     (->> rows
;;          (reduce (fn [state row]
;;                    (if (and (> (:num-rows state) 100000))
;;                      (reduced state)
;;                      (let [timestamp (.getTime ^java.sql.Timestamp (:timestamp row))
;;                            ^bytes data (:data row)
;;                            ^ModeSReply msg (decode data timestamp)]
;;                        (when (= (:num-rows state) 0)
;;                          (log "first record"))
;;                        (-> state
;;                            (update :num-rows inc)
;;                            (update-in [:msg-types (.name (.getType msg))] inc)
;;                            (update-in [:icaos (:icao row)] inc)
;;                            (cond-> (:is_mlat row)
;;                              (update :num-mlats inc))
;;                            (cond-> (and (nil? (:mlat state)) (:is_mlat row))
;;                              (assoc :mlat [msg
;;                                            (.getType msg)
;;                                            (.getDownlinkFormat msg)
;;                                            (.getFirstField msg)
;;                                            (seq data)
;;                                            (org.opensky.libadsb.tools/toHexString data)]))
;;                            (cond-> (not (.checkParity msg))
;;                              (update :num-errors inc))
;;                            (cond-> (instance? org.opensky.libadsb.msgs.AirbornePositionV0Msg msg)
;;                              (update :num-position-reports inc))))))
;;                  {:num-rows 0
;;                   :num-errors 0
;;                   :num-position-reports 0
;;                   :num-mlats 0
;;                   :msg-types {}}))))


;; (defn reduce-results2 [rows]
;;   (reduce (fn [state row]
;;             (if (> state 1000000)
;;               (reduced state)
;;               (inc state)))
;;           0
;;           rows))

(defn day-of-week
  ([ts]
   (day-of-week ts (java.util.Calendar/getInstance)))
  ([ts ^java.util.Calendar cal]
   (.setTime cal ts)
   (.get cal java.util.Calendar/DAY_OF_WEEK)))

(defn hour-of-day
  ([ts]
   (hour-of-day ts (java.util.Calendar/getInstance)))
  ([ts ^java.util.Calendar cal]
   (.setTime cal ts)
   (.get cal java.util.Calendar/HOUR_OF_DAY)))


;; (defn hourly-heatmap-by-icao [rows]
;;   (let [inc (fnil inc 0)]
;;     (reduce (fn [state row]
;;               (if (and (> (:num-rows state) 10))
;;                 (reduced state)
;;                 (let [cal (java.util.Calendar/getInstance)
;;                       dow (day-of-week (:timestamp row) cal)
;;                       hod (hour-of-day (:timestamp row) cal)]
;;                   (-> state
;;                       (update-in [:hour (:icao row) [dow hod]] inc)))))
;;             {:num-rows 0}
;;             rows)))


;; (defn hourly-stats [rows]
;;   (let [inc (fnil inc 0)]
;;     (reduce (fn [state row]
;;               (when (= (:num-rows state) 0)
;;                 (log "got first row"))
;;               (if (and false (> (:num-rows state) 10000))
;;                 (reduced state)
;;                 (let [cal (java.util.Calendar/getInstance)
;;                       dow (day-of-week (:hour row) cal)
;;                       hod (hour-of-day (:hour row) cal)]
;;                   (-> state
;;                       (update :num-rows inc)
;;                       (update-in [:hour-stats (:icao row) [dow hod]] inc)))))
;;             {:num-rows 0}
;;             rows)))


;; (defn hourly-stats-for-icao [icao]
;;   (jdbc/with-db-transaction [tx db-spec]
;;     (jdbc/query tx
;;                 [(jdbc/prepare-statement
;;                   (:connection tx)
;;                   (str "select icao, date_trunc('hour', timestamp) as hour "
;;                        "from pings where icao = ? group by icao, hour "
;;                        "order by hour asc;")
;;                   {:fetch-size 100000})
;;                  icao]
;;                 {:result-set-fn (fn [result-set]
;;                                   (hourly-stats result-set))})))


;; (defn stats-for-icao [icao]
;;   (jdbc/with-db-transaction [tx db-spec]
;;     (jdbc/query tx
;;                 [(jdbc/prepare-statement
;;                   (:connection tx)
;;                   (str "select * from pings where icao = ? order by timestamp asc;")
;;                   {:fetch-size 100000})
;;                  icao]
;;                 {:result-set-fn (fn [result-set]
;;                                   (reduce-results result-set))})))


(defn empty-state-vector []
  {:lat nil
   :lon nil
   :alt nil
   :hdg nil
   :spd nil
   :vspd nil
   :squawk nil
   :callsign nil})


(defn update-dim [sv tag val ts]
  (assoc sv tag [val ts]))


;; (def state_ (atom (statevec/initial-state)))

(defn initial-state [lat lon]
  {:num-rows 0
   :decoder (ModeSDecoder.)
   :receiver-pos (Position. lat lon 0.0)
   :num-errors 0
   :num-position-reports 0
   :num-mlats 0
   :msg-types {}
   :earliest-ts nil
   :latest-ts nil
   :state-vecs {}
   :tcas []})


(defn update-state-msg-tcas [state row ^ModeSReply msg]
  (if (= (.name (.getType msg)) "ADSB_TCAS")
    (let [^TCASResolutionAdvisoryMsg msg msg]
      (update-in state
                 [:tcas]
                 #(conj % [(:timestamp row) (:icao row) (.getThreatType msg) (.getThreatIdentity msg)])))
    state))


(defn update-state-timestamps [state row]
  (let [earliest-ts ^java.sql.Timestamp (:earliest-ts state)
        latest-ts ^java.sql.Timestamp (:latest-ts state)
        ts ^java.sql.Timestamp (:timestamp row)]
    (-> state
        (cond-> (or (nil? earliest-ts) (.before ts earliest-ts))
          (assoc :earliest-ts ts))
        (cond-> (or (nil? latest-ts) (.after ts latest-ts))
          (assoc :latest-ts ts)))))


(defn update-state-msg [state row ^ModeSReply msg]
  (let [msg-type (.name (.getType msg))]
    (-> state
        (update-in [:msg-types msg-type] (fnil inc 0))
        (cond-> (= msg-type "MODES_REPLY")
          (update-in [:unknown-df-counts (.getDownlinkFormat msg)] (fnil inc 0)))
        (cond-> (not (= msg-type "MODES_REPLY"))
          (update-in [:known-df-counts (.getDownlinkFormat msg)] (fnil inc 0)))
        (cond-> (= msg-type "ADSB_EMERGENCY")
          (update-in [:emergency-types (.getEmergencyStateText ^EmergencyOrPriorityStatusMsg msg)] (fnil inc 0)))
        (cond-> (= msg-type "ADSB_EMERGENCY")
          (update-in [:emergency-icaos (:icao row)] (fnil inc 0)))
        (update-state-msg-tcas row msg)
        (cond-> (and (nil? (:mlat state)) (:is_mlat row))
          (assoc :mlat [msg
                        (.getType msg)
                        (.getDownlinkFormat msg)
                        (.getFirstField msg)
                        (seq (:data row))
                        (org.opensky.libadsb.tools/toHexString ^bytes (:data row))]))
        (cond-> (not (.checkParity msg))
          (update :num-errors inc))
        (cond-> (instance? org.opensky.libadsb.msgs.AirbornePositionV0Msg msg)
          (update :num-position-reports inc)))))


(defn update-hour [state row]
  (-> state
      (assoc-in [:hours [(day-of-week (:timestamp row))
                         (hour-of-day (:timestamp row))]]
                true)))


(defn decode-surface-position [^ModeSDecoder decoder ^java.sql.Timestamp ts ^SurfacePositionV0Msg msg ^Position pos]
  (.decodePosition decoder (.getTime ts) msg pos))


(defn decode-airborne-position [^ModeSDecoder decoder ^java.sql.Timestamp ts ^AirbornePositionV0Msg msg ^Position pos]
  (.decodePosition decoder (.getTime ts) msg pos))


(defn update-state-msg-position [state row ^ModeSReply msg ^java.sql.Timestamp ts]
  (cond
    (instance? SurfacePositionV0Msg msg)
    (let [^Position pos (decode-surface-position (:decoder state) ts msg (:receiver-pos state))]
      (if pos
        (update-in state [:state-vecs (:icao row)]
                   update-dim :pos {:lat (.getLatitude pos) :lon (.getLongitude pos)} ts))
      state)
    (instance? AirbornePositionV0Msg msg)
    (let [^Position pos (decode-airborne-position (:decoder state) ts msg (:receiver-pos state))]
      (if pos
        (update-in state [:state-vecs (:icao row)]
                   update-dim :pos {:lat (.getLatitude pos) :lon (.getLongitude pos)} ts)
        state))
    :else state))


(defn reduce-all-results [state_ rows]
  (let [got-first?_ (atom false)
        decoder ^ModeSDecoder (:decoder @state_)
        inc (fnil inc 0)]
    (let [state (->> rows
                     (reduce (fn [state row]
                               (let [timestamp (.getTime ^java.sql.Timestamp (:timestamp row))
                                     ^bytes data (:data row)
                                     ^ModeSReply msg (.decode decoder data)]
                                 (when (= (:num-rows state) 0)
                                   (log "first record"))
                                 (let [s (-> state
                                             (update-state-msg row msg)
                                             (update-state-msg-position row msg (:timestamp row))
                                             (update-state-timestamps row)
                                             ;;(update-hour row)
                                             (update :num-rows inc)
                                             (update-in [:icaos (:icao row)] inc)
                                             (cond-> (:is_mlat row)
                                               (update :num-mlats inc)))]
                                   (reset! state_ s)
                                   s)))
                             @state_))]
      (reset! state_ (assoc state :complete? true)))))



(defn process-all
  ([state_]
   (process-all state_ {:count 1000000}))
  ([state_ options]
   (let [start-time-ms (System/currentTimeMillis)
         count (:count options)
         results (jdbc/with-db-transaction [tx db-spec]
                   (jdbc/query tx
                               [(jdbc/prepare-statement
                                 (:connection tx)
                                 (str "select * from pings order by timestamp asc limit " count ";")
                                 {:fetch-size 100000})]
                               {:result-set-fn (fn [result-set]
                                                 (reduce-all-results state_ result-set))}))
         end-time-ms (System/currentTimeMillis)
         duration-ms (- end-time-ms start-time-ms)
         num-rows (:num-rows results)]
     (log "Processed %s records in %.1f s: %.1f records/s"
          (:num-rows results)
          (/ duration-ms 1000.0)
          (/ num-rows (/ duration-ms 1000.0)))
     results)))



(defn hex-to-binary [^String hex]
  (.toString (BigInteger. hex 16) 2))

(defn binary-to-int [^String bin]
  (BigInteger. bin 2))


(defn -main [& args]
  (log "%s" args)
  (let [stats (process-all)]
    ;;(pprint/pprint stats)
    (doseq [[icao stats] (:hour-stats stats)]
      (let [records (->> stats
                         (map (fn [[[dow hour] count]] [icao dow hour count]))
                         sort)]
        (doseq [record records]
          (println (string/join "," record)))))))
