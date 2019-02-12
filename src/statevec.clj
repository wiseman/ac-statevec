(ns statevec
  ""
  (:require [cheshire.core :as cheshire]
            [clojure.java.jdbc :as jdbc]
            [clojure.pprint :as pprint]
            [clojure.string :as string]
            [compojure.route :as route]
            [compojure.core :as compojure]
            [java-time :as jt]
            [org.httpkit.server :as server])
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


;; (def state_ (atom (statevec/initial-state 34.13366 -118.19241)))
;; (statevec/start-server state_)
;; (statevec/process-all state_ {:count 10000000 :fetch-size 1000})


(def db-spec "jdbc:postgresql://wiseman@localhost:5432/orbital")


;; ------------------------------------------------------------------------
;; Utilities.
;; ------------------------------------------------------------------------

(defn log
  ([msg]
   (println (jt/format DateTimeFormatter/ISO_INSTANT (jt/instant)) msg))
  ([fmt & args]
   (log (apply format fmt args))))


(defn dbfs [v]
  (* 20 (Math/log10 (/ (max v 0.5) 255.0))))


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


(defn hex-to-binary [^String hex]
  (.toString (BigInteger. hex 16) 2))


(defn binary-to-int [^String bin]
  (BigInteger. bin 2))


(defn decode-surface-position
  [^ModeSDecoder decoder
   ^SurfacePositionV0Msg msg
   ^java.sql.Timestamp ts
   ^Position pos]
  (.decodePosition decoder (.getTime ts) msg pos))


(defn decode-airborne-position
  [^ModeSDecoder decoder
   ^AirbornePositionV0Msg msg
   ^java.sql.Timestamp ts
   ^Position pos]
  (.decodePosition decoder (.getTime ts) msg pos))


(def safe-inc (fnil inc 0))

(defn update-svv
  ([sv tag val ts]
   (update-svv sv tag val ts {}))
  ([sv tag val ts options]
   (if (:append? options)
     (update sv tag (fnil conj []) [val ts])
     (assoc sv tag [val ts]))))


(defn get-svv
  ([sv tag]
   (get-svv sv tag nil))
  ([sv tag default]
   (if sv
     (let [pair (sv tag)]
       (if pair
         (nth (sv tag) 0)
         default))
     default)))


(defn get-svv-time [sv tag]
  (nth (sv tag) 1))


(defn state->receiver-json [state_]
  (let [^Position pos (:receiver-pos state_)]
    {:lat (.getLatitude pos)
     :lon (.getLongitude pos)
     :refresh 33
     :version "1.0"
     :history 0}))


(defn state-vec-last-updated [sv]
  (get-svv-time sv :msg-count))


(defn state-vecs->json [svs now-secs]
  (map (fn [[icao sv]]
         (let [pos (get-svv sv :pos)
               ^java.sql.Timestamp last-update (state-vec-last-updated sv)
               last-update-secs (if last-update
                                  (- now-secs (/ (.getTime last-update) 1000.0))
                                  100000)]
           (-> {:hex icao
                :messages (get-svv sv :msg-count)
                :seen last-update-secs}
               (cond->
                   pos (assoc :lat (:lat pos))
                   pos (assoc :lon (:lon pos))
                   pos (assoc
                        :seen_pos
                        (- now-secs
                           (/ (.getTime ^java.sql.Timestamp (get-svv-time sv :pos))
                              1000.0)))
                   (:squawk sv) (assoc :squawk (get-svv sv :squawk))
                   (:callsign sv) (assoc :flight (get-svv sv :callsign))
                   (:hdg sv) (assoc :track (get-svv sv :hdg))
                   (:vspd sv) (assoc :vert_rate (get-svv sv :vspd))
                   (:spd sv) (assoc :speed (get-svv sv :spd))
                   (:alt sv) (assoc :altitude (get-svv sv :alt))))))
       svs))


(defn state->aircraft-json [state]
  (let [^java.sql.Timestamp latest-ts (:latest-ts state)
        now-secs (if latest-ts
                   (/ (.getTime latest-ts) 1000.0)
                   nil)]
    {:now now-secs
     :messages (:num-rows state)
     :aircraft (state-vecs->json (:state-vecs state) now-secs)}))


(defn update-ac-svv
  ([state rec tag val]
   (update-ac-svv state rec tag val {}))
  ([state rec tag val options]
   (assert (:icao rec))
   (update-in state [:state-vecs (:icao rec)]
              update-svv tag val (:timestamp rec) options)))


;; ------------------------------------------------------------------------
;; Message handlers.
;; ------------------------------------------------------------------------

(defmulti update-state-for-msg (fn [state msg rec] (class msg)))


(defmacro defmsghandler [type args & body]
  (let [args [(nth args 0) (with-meta (nth args 1) {:tag type}) (nth args 2)]]
    `(defmethod update-state-for-msg ~type ~args ~@body)))


(defmsghandler org.opensky.libadsb.msgs.AirborneOperationalStatusV1Msg [state msg rec]
  state)


(defmsghandler org.opensky.libadsb.msgs.AirborneOperationalStatusV2Msg [state msg rec]
  state)


(defmsghandler org.opensky.libadsb.msgs.AllCallReply [state msg rec]
  state)


(defmsghandler org.opensky.libadsb.msgs.TCASResolutionAdvisoryMsg [state msg rec]
  (update-ac-svv state rec :tcas (str msg) {:append? true}))


(defmsghandler org.opensky.libadsb.msgs.AltitudeReply [state msg rec]
  (update-ac-svv state rec :alt (.getAltitude msg)))


(defmsghandler org.opensky.libadsb.msgs.CommBAltitudeReply [state msg rec]
  (update-ac-svv state rec :alt (.getAltitude msg)))


(defmsghandler org.opensky.libadsb.msgs.CommBIdentifyReply [state msg rec]
  (update-ac-svv state rec :squawk (.getIdentity msg)))


(defmsghandler org.opensky.libadsb.msgs.CommDExtendedLengthMsg [state msg rec]
  state)


;;(defmsghandler org.opensky.libadsb.msgs.ExtendedSquitter [state msg rec]
;;  state)


(defmsghandler org.opensky.libadsb.msgs.IdentifyReply [state msg rec]
  (update-ac-svv state rec :squawk (.getIdentity msg)))


;;(defmsghandler org.opensky.libadsb.msgs.ModeSReply [state msg rec]
;;  state)


(defmsghandler org.opensky.libadsb.msgs.ShortACAS [state msg rec]
  (update-ac-svv state rec :alt (.getAltitude msg)))


(defmsghandler org.opensky.libadsb.msgs.LongACAS [state msg rec]
  (-> state
      (update-ac-svv rec :alt (.getAltitude msg))
      (cond-> (.hasValidRAC msg)
        (update-ac-svv
         rec :advisory
         {:no-pass-below (.noPassBelow msg)
          :no-pass-above (.noPassAbove msg)
          :no-turn-left (.noTurnLeft msg)
          :no-turn-right (.noTurnRight msg)
          :multiple-threats? (.hasMultipleThreats msg)
          :terminated? (.hasTerminated msg)}
         {:append? true}))))


(defmsghandler org.opensky.libadsb.msgs.VelocityOverGroundMsg [state msg rec]
  (-> state
      (update-ac-svv rec :vspd (.getVerticalRate msg))
      (update-ac-svv rec :hdg (.getHeading msg))
      (update-ac-svv rec :spd (.getVelocity msg))))


(defmsghandler org.opensky.libadsb.msgs.IdentificationMsg [state msg rec]
  (update-ac-svv
   state rec :callsign (String. (.getIdentity msg))))>


(defmsghandler org.opensky.libadsb.msgs.AirspeedHeadingMsg [state msg rec]
  (-> state
      (cond-> (.hasAirspeedInfo msg)
        (update-ac-svv rec :air-spd (.getAirspeed msg)))
      (cond-> (.hasVerticalRateInfo msg)
        (update-ac-svv rec :vspd (.getVerticalRate msg)))
      (cond-> (.hasHeadingStatusFlag msg)
        (update-ac-svv rec :hdg (.getHeading msg)))))


(defmsghandler SurfacePositionV0Msg [state msg rec]
  (let [ts (:timestamp rec)
        ^Position pos (decode-surface-position
                       (:decoder state)
                       msg
                       ts
                       (:receiver-pos state))]
    (if pos
      (update-ac-svv
       state rec :pos {:lat (.getLatitude pos) :lon (.getLongitude pos)})
      state)))


(defmsghandler AirbornePositionV0Msg [state msg rec]
  (let [ts (:timestamp rec)
        ^Position pos (decode-airborne-position
                       (:decoder state)
                       msg
                       ts
                       (:receiver-pos state))]
    (if pos
      (update-ac-svv
       state rec :pos {:lat (.getLatitude pos) :lon (.getLongitude pos)})
      state)))


(defmsghandler EmergencyOrPriorityStatusMsg [state msg rec]
  (-> state
      (update-in
       [:emergency-types (.getEmergencyStateText msg)]
       safe-inc)
      (update-in
       [:emergency-icaos (:icao rec)]
       safe-inc)))


(defn initial-state [lat lon]
  {:num-rows 0
   :decoder (ModeSDecoder.)
   :receiver-pos (Position. lon lat 0.0)
   :num-errors 0
   :num-position-reports 0
   :num-mlats 0
   :msg-types {}
   :msg-classes {}
   :earliest-ts nil
   :latest-ts nil
   :state-vecs {}})


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
        (update-in [:msg-types msg-type] safe-inc)
        (update-in [:msg-classes (class msg)] safe-inc)
        (update-ac-svv
         row
         :msg-count
         (safe-inc (get-svv (get-in state [:state-vecs (:icao row)]) :msg-count 0)))
        (update-ac-svv row :rssi (dbfs (:signal_level row)))
        (cond-> (not (or (= msg-type "MODES_REPLY")
                         (= msg-type "EXTENDED_SQUITTER")))
          (update-state-for-msg msg row))
        (cond-> (= msg-type "MODES_REPLY")
          (update-in [:unknown-df-counts (.getDownlinkFormat msg)] safe-inc))
        (cond-> (not (= msg-type "MODES_REPLY"))
          (update-in [:known-df-counts (.getDownlinkFormat msg)] safe-inc))
        (cond-> (and (nil? (:mlat state)) (:is_mlat row))
          (assoc :mlat [msg
                        (.getType msg)
                        (.getDownlinkFormat msg)
                        (.getFirstField msg)
                        (seq (:data row))
                        (org.opensky.libadsb.tools/toHexString ^bytes (:data row))]))
        (cond-> (not (.checkParity msg))
          (update :num-errors inc)))))


(defn update-hour [state row]
  (-> state
      (assoc-in [:hours [(day-of-week (:timestamp row))
                         (hour-of-day (:timestamp row))]]
                true)))


(def record-flights-interval 10000)
(def flight-timeout (* 10 60 1000))


(defn record-flights [state]
  (let [^java.sql.Timestamp latest-ts (:latest-ts state)
        ^java.sql.Timestamp record-flights-check-ts (::record-flights-check-ts state)]
    (cond
      ;; Haven't checked for flights yet;
      (nil? record-flights-check-ts)
      (assoc state ::record-flights-check-ts latest-ts)
      ;; Have checked for flights recently:
      (< (- (.getTime latest-ts) (.getTime record-flights-check-ts)) record-flights-interval)
      state
      ;; Time to check for flights:
      :else
      (do (log "recording flights")
          (-> state
              (assoc ::record-flights-check-ts latest-ts)
              (update
               :state-vecs
               (fn update-svs [svs]
                 (into
                  {}
                  (filter (fn woo [[icao sv]]
                            (let [^java.sql.Timestamp last-updated (state-vec-last-updated sv)]
                              (if (>= (- (.getTime latest-ts) (.getTime last-updated)) flight-timeout)
                                (do (log "Saving off flight for %s (%s messages) at %s"
                                         icao
                                         (get-svv sv :msg-count)
                                         latest-ts)
                                    false)
                                true)))
                          svs)))))))))


(defn update-state [^ModeSDecoder decoder state row]
  (let [timestamp (.getTime ^java.sql.Timestamp (:timestamp row))
        ^bytes data (:data row)
        ^ModeSReply msg (.decode decoder data)]
    (when (= (:num-rows state) 0)
      (log "Got first record"))
    (-> state
        (update-state-msg row msg)
        (update-state-timestamps row)
        (update :num-rows safe-inc)
        (update-in [:icaos (:icao row)] safe-inc)
        (cond-> (:is_mlat row)
          (update :num-mlats safe-inc))
        record-flights)))


(defn reduce-all-results [state_ rows]
  (let [got-first?_ (atom false)
        decoder ^ModeSDecoder (:decoder @state_)]
    (let [state (reduce (fn [state row]
                          (reset! state_ (update-state decoder state row)))
                        @state_
                        rows)]
      (reset! state_ (assoc state :processing-complete? true)))))


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
                                 {:fetch-size (or (:fetch-size options) 100000)})]
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


(defn serve-receiver-json [state_]
  {:status 200
   :headers {"Content-Type" "application/json; charset=utf-8"}
   :body (-> @state_
             state->receiver-json
             cheshire/generate-string)})


(defn serve-aircraft-json [state_]
  {:status 200
   :headers {"Content-Type" "application/json; charset=utf-8"}
   :body (-> @state_
             state->aircraft-json
             cheshire/generate-string)})

(defonce server_ (atom nil))


(defn start-server [state_]
  (when @server_
    (@server_))
  (let [app (compojure/routes
             (compojure/GET "/data/receiver.json" [] (fn [req] (serve-receiver-json state_)))
             (compojure/GET "/data/aircraft.json" [] (fn [req] (serve-aircraft-json state_)))
             (route/files "/"))]
    (reset! server_ (server/run-server app {:port 8080}))))


(defn stop-server []
  (when @server_
    (@server_)))


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
