(ns statevec
  ""
  (:require
   [cheshire.core :as cheshire]
   [clojure.core.async :as a]
   [clojure.java.jdbc :as jdbc]
   [clojure.pprint :as pprint]
   [clojure.set :as set]
   [clojure.set :as set]
   [clojure.string :as string]
   [com.lemonodor.gflags :as gflags]
   [compojure.core :as compojure]
   [compojure.route :as route]
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


(gflags/define-string "db-spec"
  "jdbc:postgresql://wiseman@localhost:5432/orbital"
  "The JDBC DB spec to use.")

(gflags/define-integer "fetch-size"
  100000
  "The DB fetch size to use.")

(gflags/define-integer "max-num-records"
  nil
  "The max number of records to process")

(gflags/define-string "start-time"
  nil
  "Start time, e.g. '2018-04-14 00:00:00'")

(gflags/define-string "end-time"
  nil
  "End time, e.g. '2018-04-14 00:00:00'")

(gflags/define-string "icaos"
  nil
  "List of ICAOs, e.g. 'AE0000,FFFFFF'")


;; ------------------------------------------------------------------------
;; Utilities.
;; ------------------------------------------------------------------------

(defn log
  ([msg]
   (println (jt/format DateTimeFormatter/ISO_INSTANT (jt/instant)) msg))
  ([fmt & args]
   (log (apply format fmt args))))


(defn dbfs [v]
  (* 20 (Math/log10 ^double (/ (double (max v 0.5)) 255.0))))


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

(defn update-state-vector-value
  ([sv tag val ts]
   (update-state-vector-value sv tag val ts {}))
  ([sv tag val ts options]
   (if (:append? options)
     (update sv tag (fnil conj []) [val ts])
     (assoc sv tag [val ts]))))


(defn get-state-vector-value
  ([sv tag]
   (get-state-vector-value sv tag nil))
  ([sv tag default]
   (if sv
     (let [pair (sv tag)]
       (if pair
         (nth (sv tag) 0)
         default))
     default)))


(defn get-state-vector-value-time [sv tag]
  (nth (sv tag) 1))


(defn state->receiver-json [state_]
  (let [^Position pos (:receiver-pos state_)]
    {:lat (.getLatitude pos)
     :lon (.getLongitude pos)
     :refresh 33
     :version "1.0"
     :history 0}))


(defn state-vec-last-updated [sv]
  (get-state-vector-value-time sv :msg-count))


;; ------------------------------------------------------------------------
;; Track detectors
;; ------------------------------------------------------------------------

(defn curviness ^double [bearings]
  (Math/abs
   ^double (reduce (fn [^double sum [^double a ^double b]]
                     (+ sum (- a b)))
                   0.0
                   (partition 2 1 bearings))))


(defn too-old? [datum ^java.sql.Timestamp now max-age-ms]
  (let [then ^java.sql.Timestamp (:timestamp datum)
        time-diff-ms (- (.getTime now) (.getTime then))]
    (> time-diff-ms max-age-ms)))


(defn distance [pos1 pos2]
  (let [earth-radius 6372.8 ;; km
        sin2 (fn sin2 ^double [^double theta] (* (Math/sin theta) (Math/sin theta)))
        alpha (fn alpha ^double [^double lat1 ^double lat2 ^double delta-lat ^double delta-lon]
                (+ (sin2 (/ delta-lat 2.0))
                   (* (sin2 (/ delta-lon 2)) (Math/cos lat1) (Math/cos lat2))))
        {lat1 :lat lon1 :lon} pos1
        {lat2 :lat lon2 :lon} pos2
        delta-lat (Math/toRadians (- ^double lat2 ^double lat1))
        delta-lon (Math/toRadians (- ^double lon2 ^double lon1))
        lat1 (Math/toRadians lat1)
        lat2 (Math/toRadians lat2)]
    (* earth-radius 2
       (Math/asin (Math/sqrt (alpha lat1 lat2 delta-lat delta-lon))))))


(defn bearing [pos1 pos2]
  (let [{lat1 :lat lon1 :lon} pos1
        {lat2 :lat lon2 :lon} pos2
        lat1 ^double (Math/toRadians lat1)
        lat2 ^double (Math/toRadians lat2)
        lon-diff ^double (Math/toRadians (- ^double lon2 ^double lon1))
        y ^double (* (Math/sin lon-diff) (Math/cos lat2))
        x ^double (- (* (Math/cos lat1) (Math/sin lat2))
                     (* (Math/sin lat1) (Math/cos lat2) (Math/cos lon-diff)))]
    (mod (+ (Math/toDegrees (Math/atan2 y x)) 360.0) 360.0)))


(defn add-position-bearing-and-distance [hist pos ts]
  (let [last-pos (last (:positions hist))
        dist (distance last-pos pos)]
    (-> hist
        (update :positions conj (assoc pos :timestamp ts))
        (update :bearings  conj {:timestamp ts :bearing (bearing last-pos pos)})
        (update :distances conj {:timestamp ts :distance dist}))))


(def window-duration-ms (* 5 60 1000))


(defn log-orbits [state]
  (doseq [[icao hist] state]
    (let [c (->> hist :bearings (map :bearing) curviness)
          d (->> hist :distances (map :distance) (apply +))]
      (when (> c 360.0)
        (log "AWW %s %s %s %s" (->> hist :bearings first :timestamp) icao d c)))))


(defn update-orbit-state [state rec pos]
  (let [icao (:icao rec)
        ts (:timestamp rec)
        hist (get state icao nil)]
    (if (nil? hist)
      (assoc state icao {:positions [(assoc pos :timestamp ts)]
                         :bearings []
                         :distances []})
      (let [age-filter (complement #(too-old? % ts window-duration-ms))
            new-state
            (assoc state icao (-> hist
                                  (add-position-bearing-and-distance pos ts)
                                  (update :positions #(->> % (filter age-filter) (into [])))
                                  (update :bearings #(->> % (filter age-filter) (into [])))
                                  (update :distances #(->> % (filter age-filter) (into [])))))]
        (log-orbits new-state)
        new-state))))



(def ac-event-chan (a/chan 1000))

(def orbit-state_ (atom {}))


(defn orbit-detector []
  (reset! orbit-state_ {})
  (a/go-loop []
    (let [[rec tag val] (a/<! ac-event-chan)]
      (when (= tag :pos)
        (swap! orbit-state_ update-orbit-state rec val))
      (recur)))
  ac-event-chan)


(defn update-aircraft-state-vector-value
  ([state rec tag val]
   (update-aircraft-state-vector-value state rec tag val {}))
  ([state rec tag val options]
   (assert (:icao rec))
   (a/>!! ac-event-chan [rec tag val])
   (update-in state [:state-vecs (:icao rec)]
              update-state-vector-value tag val (:timestamp rec) options)))


;; ------------------------------------------------------------------------
;; Message handlers.
;; ------------------------------------------------------------------------

(defmulti update-state-for-msg (fn [state msg rec] (class msg)))


(defmacro defmsghandler [type args & body]
  (let [args [(nth args 0) (with-meta (nth args 1) {:tag type}) (nth args 2)]]
    `(defmethod update-state-for-msg ~type ~args ~@body)))


(defmsghandler org.opensky.libadsb.msgs.OperationalStatusV0Msg [state msg rec]
  state)


(defmsghandler org.opensky.libadsb.msgs.AirborneOperationalStatusV1Msg [state msg rec]
  state)


(defmsghandler org.opensky.libadsb.msgs.AirborneOperationalStatusV2Msg [state msg rec]
  state)

(defmsghandler org.opensky.libadsb.msgs.SurfaceOperationalStatusV1Msg [state msg rec]
  state)

(defmsghandler org.opensky.libadsb.msgs.SurfaceOperationalStatusV2Msg [state msg rec]
  state)

(defmsghandler org.opensky.libadsb.msgs.AllCallReply [state msg rec]
  state)


(defmsghandler org.opensky.libadsb.msgs.TCASResolutionAdvisoryMsg [state msg rec]
  (update-aircraft-state-vector-value state rec :tcas (str msg) {:append? true}))


(defmsghandler org.opensky.libadsb.msgs.AltitudeReply [state msg rec]
  (update-aircraft-state-vector-value state rec :alt (.getAltitude msg)))


(defmsghandler org.opensky.libadsb.msgs.CommBAltitudeReply [state msg rec]
  (update-aircraft-state-vector-value state rec :alt (.getAltitude msg)))


(defmsghandler org.opensky.libadsb.msgs.CommBIdentifyReply [state msg rec]
  (update-aircraft-state-vector-value state rec :squawk (.getIdentity msg)))


(defmsghandler org.opensky.libadsb.msgs.CommDExtendedLengthMsg [state msg rec]
  state)


;;(defmsghandler org.opensky.libadsb.msgs.ExtendedSquitter [state msg rec]
;;  state)


(defmsghandler org.opensky.libadsb.msgs.IdentifyReply [state msg rec]
  (update-aircraft-state-vector-value state rec :squawk (.getIdentity msg)))


;;(defmsghandler org.opensky.libadsb.msgs.ModeSReply [state msg rec]
;;  state)


(defmsghandler org.opensky.libadsb.msgs.ShortACAS [state msg rec]
  (update-aircraft-state-vector-value state rec :alt (.getAltitude msg)))


(defmsghandler org.opensky.libadsb.msgs.LongACAS [state msg rec]
  (-> state
      (update-aircraft-state-vector-value rec :alt (.getAltitude msg))
      (cond-> (.hasValidRAC msg)
        (update-aircraft-state-vector-value
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
      (update-aircraft-state-vector-value rec :vspd (.getVerticalRate msg))
      (update-aircraft-state-vector-value rec :hdg (.getHeading msg))
      (update-aircraft-state-vector-value rec :spd (.getVelocity msg))))


(defmsghandler org.opensky.libadsb.msgs.IdentificationMsg [state msg rec]
  (update-aircraft-state-vector-value
   state rec :callsign (String. (.getIdentity msg))))>


(defmsghandler org.opensky.libadsb.msgs.AirspeedHeadingMsg [state msg rec]
  (-> state
      (cond-> (.hasAirspeedInfo msg)
        (update-aircraft-state-vector-value rec :air-spd (.getAirspeed msg)))
      (cond-> (.hasVerticalRateInfo msg)
        (update-aircraft-state-vector-value rec :vspd (.getVerticalRate msg)))
      (cond-> (.hasHeadingStatusFlag msg)
        (update-aircraft-state-vector-value rec :hdg (.getHeading msg)))))


(defmsghandler SurfacePositionV0Msg [state msg rec]
  (let [ts (:timestamp rec)
        ^Position pos (decode-surface-position
                       (:decoder state)
                       msg
                       ts
                       (:receiver-pos state))]
    (if pos
      (update-aircraft-state-vector-value
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
      (update-aircraft-state-vector-value
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
        (update-aircraft-state-vector-value
         row
         :msg-count
         (safe-inc (get-state-vector-value (get-in state [:state-vecs (:icao row)]) :msg-count 0)))
        (update-aircraft-state-vector-value row :rssi (dbfs (:signal_level row)))
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
          (update :num-party-errors safe-inc)))))


(defn update-hour [state row]
  (-> state
      (assoc-in [:hours [(day-of-week (:timestamp row))
                         (hour-of-day (:timestamp row))]]
                true)))


(def record-flights-interval (* 60 1000))
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
      (do (log "Woop %s %s" latest-ts (:num-errors state))
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
                                (do
                                  ;;(log "Saving off flight for %s (%s messages) at %s" icao (get-state-vector-value sv :msg-count) latest-ts)
                                  false)
                                true)))
                          svs)))))))))


(defn update-state [^ModeSDecoder decoder state row]
  (when (= (:num-rows state) 0)
    (log "Got first record"))
  (let [timestamp (.getTime ^java.sql.Timestamp (:timestamp row))
        ^bytes data (:data row)]
    (try (let [^ModeSReply msg (.decode decoder data)]
           (-> state
               (update-state-msg row msg)
               (update-state-timestamps row)
               (update :num-rows safe-inc)
               (update-in [:icaos (:icao row)] safe-inc)
               (cond-> (:is_mlat row)
                 (update :num-mlats safe-inc))
               record-flights))
         (catch org.opensky.libadsb.exceptions.BadFormatException e
           (log "Decoding error for message %s: %s"
                (org.opensky.libadsb.tools/toHexString data)
                e)
           (update state :num-errors safe-inc)))))



(defn reduce-all-results [state_ rows]
  (let [got-first?_ (atom false)
        decoder ^ModeSDecoder (:decoder @state_)]
    (let [state (reduce (fn [state row]
                          (reset! state_ (update-state decoder state row)))
                        @state_
                        rows)]
      (reset! state_ (assoc state :processing-complete? true)))))


;; Turns this:
;;
;; {:start-time "2019-01-01 00:00:00-08"
;;  :end-time "2019-01-01 23:59:59-08"
;;  :icaos ["AE0000"]
;;  :max-num-records 10000}
;;
;; Into this:
;;
;; select * from pings
;;   where timestamp > '2019-01-01 00:00:00-08' and
;;         timestamp < '2019-01-01 23:59:59-08' and
;;         icaos in ('AE0000')
;;   order by timestamp asc
;;   limit 10000
;;
;; Janky, but that's OK.

(defn build-query [options]
  (let [{:keys [start-time end-time max-num-records icaos]} options
        filters (filter
                 identity
                 [(if start-time (format "timestamp >= '%s'" start-time))
                  (if end-time (format "timestamp <= '%s'" end-time))
                  (if icaos (format "icao in (%s)"
                                    (string/join ","
                                                 (map #(format "'%s'" %) icaos))))])
        filter-str (if (seq filters)
                     (str "where " (string/join " and " filters))
                     nil)]
    (cond-> "select * from pings"
      filter-str (str " " filter-str)
      true (str " order by timestamp asc")
      max-num-records (str " limit " max-num-records))))


(defn process-all
  ([state_]
   (process-all state_ {}))
  ([state_ options]
   (let [start-time-ms (System/currentTimeMillis)
         query (build-query options)
         _ (log "Using query: %s" query)
         results (jdbc/with-db-transaction [tx (gflags/flags :db-spec)]
                   (jdbc/query tx
                               [(jdbc/prepare-statement
                                 (:connection tx)
                                 query
                                 {:fetch-size (or (:fetch-size options) 100000)})]
                               {:result-set-fn (fn [result-set]
                                                 (reduce-all-results state_ result-set))}))
         end-time-ms (System/currentTimeMillis)
         duration-ms (- end-time-ms start-time-ms)
         num-rows (:num-rows results)]
     (log "Processed %s records in %.1f s (%.1f records/s)"
          (:num-rows results)
          (/ duration-ms 1000.0)
          (/ num-rows (/ duration-ms 1000.0)))
     results)))


(defmulti state-vec->json (fn [kind sv icao now-secs] kind))


(defmethod state-vec->json :dump1090-mutability [kind sv icao now-secs]
  (let [pos (get-state-vector-value sv :pos)
        ^java.sql.Timestamp last-update (state-vec-last-updated sv)
        last-update-secs (if last-update
                           (- now-secs (/ (.getTime last-update) 1000.0))
                           100000)]
    (-> {:hex icao
         :messages (get-state-vector-value sv :msg-count)
         :seen last-update-secs
         :rssi (get-state-vector-value sv :rssi)}
        (cond->
            pos (assoc :lat (:lat pos))
            pos (assoc :lon (:lon pos))
            pos (assoc
                 :seen_pos
                 (- now-secs
                    (/ (.getTime ^java.sql.Timestamp (get-state-vector-value-time sv :pos))
                       1000.0)))
            (:squawk sv) (assoc :squawk (get-state-vector-value sv :squawk))
            (:callsign sv) (assoc :flight (get-state-vector-value sv :callsign))
            (:hdg sv) (assoc :track (get-state-vector-value sv :hdg))
            (:vspd sv) (assoc :vert_rate (get-state-vector-value sv :vspd))
            (:spd sv) (assoc :speed (get-state-vector-value sv :spd))
            (:alt sv) (assoc :altitude (get-state-vector-value sv :alt))))))


;; FlightAware's dump1090 uses almost the same JSON. The main
;; differences seem to be (1) some keys have different names and (2)
;; mlat- and tis-b- sourced info is called out.

(defmethod state-vec->json :dump1090-fa [kind sv icao now-secs]
  (let [json (state-vec->json :dump1090-mutability sv icao now-secs)]
    (set/rename-keys
     json
     {:altitude :alt_baro
      :speed :gs
      :vert_rate :baro_rate})))


(defn state-vecs->json [kind svs now-secs]
  (map (fn [[icao sv]]
         (state-vec->json kind sv icao now-secs))
       svs))


(defn state->aircraft-json [kind state]
  (let [^java.sql.Timestamp latest-ts (:latest-ts state)
        now-secs (if latest-ts
                   (/ (.getTime latest-ts) 1000.0)
                   nil)]
    {:now now-secs
     :messages (:num-rows state)
     :aircraft (state-vecs->json kind (:state-vecs state) now-secs)}))


(defn serve-receiver-json [state_]
  {:status 200
   :headers {"Content-Type" "application/json; charset=utf-8"}
   :body (-> @state_
             state->receiver-json
             cheshire/generate-string)})


(defn serve-aircraft-json [kind state_]
  (let [json-gen (partial state->aircraft-json kind)]
    {:status 200
     :headers {"Content-Type" "application/json; charset=utf-8"}
     :body (-> @state_
               json-gen
               cheshire/generate-string)}))

(defonce server_ (atom nil))


(defn start-server [state_]
  (when @server_
    (@server_))
  (let [app (compojure/routes
             (compojure/GET "/*/data/receiver.json" [] (fn [req] (serve-receiver-json state_)))
             (compojure/GET "/1/data/aircraft.json" []
                            (fn [req] (serve-aircraft-json :dump1090-mutability state_)))
             (compojure/GET "/2/data/aircraft.json" []
                            (fn [req] (serve-aircraft-json :dump1090-fa state_)))
             (route/files "/1/" {:root "public/html-dump1090-mutability"})
             (route/files "/2/" {:root "public/html-dump1090-fa"}))]
    (reset! server_ (server/run-server app {:port 8080}))))


(defn stop-server []
  (when @server_
    (@server_)))


(defn -main [& args]
  (gflags/parse-flags (into ["argv0"] args))
  (let [fetch-size (gflags/flags :fetch-size)
        start-time (gflags/flags :start-time)
        end-time (gflags/flags :end-time)
        icaos-str (gflags/flags :icaos)
        max-num-records (gflags/flags :max-num-records)
        options (cond-> {}
                  fetch-size (assoc :fetch-size fetch-size)
                  start-time (assoc :start-time start-time)
                  end-time (assoc :end-time end-time)
                  max-num-records (assoc :max-num-records max-num-records)
                  icaos-str (assoc :icaos (-> icaos-str string/upper-case (string/split #","))))]
    (orbit-detector)
    (let [state_ (atom (statevec/initial-state 34.13366 -118.19241))]
      (statevec/start-server state_)
      (statevec/process-all state_ options)
      (log "Found %s distinct ICAOs" (count (:state-vecs @state_)))))
  (doseq [[k hist] @orbit-state_]
    (let [d (->> hist :distances (map :distance) (apply +))
          c (->> hist :bearings (map :bearing) curviness)]
      (println k d c (/ c d))))
  (System/exit 0))
