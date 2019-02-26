(ns statevec
  ""
  (:require
   [cheshire.core :as json]
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


(defn dbfs
  "Converts a value in the range 0-255 to dB full-scale."
  [v]
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


(defn ts->ms
  "Converts a Timestamp to milliseconds."
  ^long [^java.sql.Timestamp ts]
  (.getTime ts))


(defn ts-diff
  "Returns the time difference in milliseconds between two Timestamps."
  ^long [ts1 ts2]
  (- (ts->ms ts1) (ts->ms ts2)))


(defn decode-surface-position
  [^ModeSDecoder decoder
   ^SurfacePositionV0Msg msg
   ts
   ^Position pos]
  (.decodePosition decoder (ts->ms ts) msg pos))


(defn decode-airborne-position
  [^ModeSDecoder decoder
   ^AirbornePositionV0Msg msg
   ts
   ^Position pos]
  (.decodePosition decoder (ts->ms ts) msg pos))


(def safe-inc (fnil inc 0))

(def safe-conj (fnil conj []))


;; State vector values have a :value and a :timestamp.

(defn update-statevec-value
  ([sv tag val ts]
   (update-statevec-value sv tag val ts {}))
  ([sv tag val ts options]
   (let [datum {:value val :timestamp ts}]
     (if (:append? options)
       (update sv tag safe-conj datum)
       (assoc sv tag datum)))))


(defn get-statevec-value
  ([sv tag]
   (get-statevec-value sv tag nil))
  ([sv tag default]
   (get-in sv [tag :value] default)))


(defn get-statevec-value-time ^java.sql.Timestamp [sv tag]
  (get-in sv [tag :timestamp]))


(defn state->receiver-json [state_]
  (let [^Position pos (:receiver-pos state_)]
    {:lat (.getLatitude pos)
     :lon (.getLongitude pos)
     :refresh 33
     :version "1.0"
     :history 0}))


(defn statevec-last-updated [sv]
  (get-statevec-value-time sv :msg-count))


;; ------------------------------------------------------------------------
;; Track detectors
;; ------------------------------------------------------------------------

(defn bearing-diff [a b]
  (- 180.0 (mod (+ (- a b) 180.0) 360.0)))


(defn curviness ^double [bearings]
  (Math/abs
   ^double (reduce (fn [^double sum [^double a ^double b]]
                     (+ sum (bearing-diff a b)))
                   0.0
                   (partition 2 1 bearings))))


(defn too-old? [datum ^java.sql.Timestamp now max-age-ms]
  (let [then (:timestamp datum)
        time-diff-ms (ts-diff now then)]
    (> time-diff-ms max-age-ms)))


(defn distance
  "Returns the distance between two positions, in km."
  [pos1 pos2]
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


(defn bearing
  "Returns the bearing from one position to another, in degrees."
  [pos1 pos2]
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


;; (def window-duration-ms (* 5 60 1000))


;; (defn log-orbits [state]
;;   (doseq [[icao hist] state]
;;     (let [c (->> hist :bearings (map :bearing) curviness)
;;           d (->> hist :distances (map :distance) (apply +))]
;;       (when (> c 360.0)
;;         (log "AWW %s %s %s %s" (->> hist :bearings first :timestamp) icao d c)))))


(def flight-chan (a/chan 1000))

(def flights_ (atom []))

(def counter_ (atom 0))


(defn history->geojson [history]
  {"type" "LineString"
   "coordinates" (map (fn [pos]
                        [(:lon pos) (:lat pos)])
                      history)})


(defn flight-curviness [flight]
  (->> (partition 2 1 flight)
       (map #(apply bearing %))
       curviness))


(defn flight-distance [flight]
  (->> (partition 2 1 flight)
       (map #(apply distance %))
       (apply +)))


(defn flight-normalized-curviness [flight]
  (/ (flight-curviness flight)
     (flight-distance flight)))


(defn flight-info [flight]
  (format
   "Total distance: %.1f, curviness: %.1f normalized curviness: %.2f"
   (float (flight-distance flight))
   (float (flight-curviness flight))
   (/ (flight-curviness flight) (flight-distance flight))))


(defn write-flight-geojson! [icao history]
  (let [path (format "geojson/%s-%s-%s-%s.geojson"
                     icao
                     (jt/format "yyyy-MM-dd'T'HH:mm:ss" (jt/local-date-time (:timestamp (first history))))
                     (int (flight-curviness history))
                     @counter_)]
    (swap! counter_ inc)
    (log "Writing %s" path)
    (spit path (json/generate-string (history->geojson history) {:pretty true}))))


(defn flight-recorder []
  (reset! flights_ [])
  (a/go-loop []
    (let [[icao history] (a/<! flight-chan)]
      (write-flight-geojson! icao history)
      (swap! flights_
             conj
             [icao history])
      (recur)))
  flight-chan)


(defn update-aircraft-statevec-value
  ([state rec tag val]
   (update-aircraft-statevec-value state rec tag val {}))
  ([state rec tag val options]
   (assert (:icao rec))
   (update-in state
              [:aircraft (:icao rec) :statevec]
              update-statevec-value tag val (:timestamp rec) options)))


(defn update-aircraft-pos [state rec ^Position pos]
  (let [datum {:lat (.getLatitude pos)
               :lon (.getLongitude pos)
               :timestamp (:timestamp rec)}
        icao (:icao rec)]
    (-> state
        (update-aircraft-statevec-value rec :pos datum)
        (update-in [:aircraft icao :history] safe-conj datum))))


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
  (update-aircraft-statevec-value state rec :tcas (str msg) {:append? true}))


(defmsghandler org.opensky.libadsb.msgs.AltitudeReply [state msg rec]
  (update-aircraft-statevec-value state rec :alt (.getAltitude msg)))


(defmsghandler org.opensky.libadsb.msgs.CommBAltitudeReply [state msg rec]
  (update-aircraft-statevec-value state rec :alt (.getAltitude msg)))


(defmsghandler org.opensky.libadsb.msgs.CommBIdentifyReply [state msg rec]
  (update-aircraft-statevec-value state rec :squawk (.getIdentity msg)))


(defmsghandler org.opensky.libadsb.msgs.CommDExtendedLengthMsg [state msg rec]
  state)


;;(defmsghandler org.opensky.libadsb.msgs.ExtendedSquitter [state msg rec]
;;  state)


(defmsghandler org.opensky.libadsb.msgs.IdentifyReply [state msg rec]
  (update-aircraft-statevec-value state rec :squawk (.getIdentity msg)))


;;(defmsghandler org.opensky.libadsb.msgs.ModeSReply [state msg rec]
;;  state)


(defmsghandler org.opensky.libadsb.msgs.ShortACAS [state msg rec]
  (update-aircraft-statevec-value state rec :alt (.getAltitude msg)))


(defmsghandler org.opensky.libadsb.msgs.LongACAS [state msg rec]
  (-> state
      (update-aircraft-statevec-value rec :alt (.getAltitude msg))
      (cond-> (.hasValidRAC msg)
        (update-aircraft-statevec-value
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
      (update-aircraft-statevec-value rec :vspd (.getVerticalRate msg))
      (update-aircraft-statevec-value rec :hdg (.getHeading msg))
      (update-aircraft-statevec-value rec :spd (.getVelocity msg))))


(defmsghandler org.opensky.libadsb.msgs.IdentificationMsg [state msg rec]
  (update-aircraft-statevec-value
   state rec :callsign (String. (.getIdentity msg))))>


(defmsghandler org.opensky.libadsb.msgs.AirspeedHeadingMsg [state msg rec]
  (-> state
      (cond-> (.hasAirspeedInfo msg)
        (update-aircraft-statevec-value rec :air-spd (.getAirspeed msg)))
      (cond-> (.hasVerticalRateInfo msg)
        (update-aircraft-statevec-value rec :vspd (.getVerticalRate msg)))
      (cond-> (.hasHeadingStatusFlag msg)
        (update-aircraft-statevec-value rec :hdg (.getHeading msg)))))


(defmsghandler SurfacePositionV0Msg [state msg rec]
  (let [ts (:timestamp rec)
        ^Position pos (decode-surface-position
                       (:decoder state)
                       msg
                       ts
                       (:receiver-pos state))]
    (if pos
      (update-aircraft-statevec-value
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
      (update-aircraft-pos state rec pos)
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
   :aircraft {}})


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
        ;;(update-in [:msg-types msg-type] safe-inc)
        ;;(update-in [:msg-classes (class msg)] safe-inc)
        (update-aircraft-statevec-value
         row
         :msg-count
         (safe-inc (get-statevec-value (get-in state [:state-vecs (:icao row)]) :msg-count 0)))
        (update-aircraft-statevec-value row :rssi (dbfs (:signal_level row)))
        (cond-> (not (or (= msg-type "MODES_REPLY")
                         (= msg-type "EXTENDED_SQUITTER")))
          (update-state-for-msg msg row))
        (cond-> (= msg-type "MODES_REPLY")
          (update-in [:unknown-df-counts (.getDownlinkFormat msg)] safe-inc))
        (cond-> (not (= msg-type "MODES_REPLY"))
          (update-in [:known-df-counts (.getDownlinkFormat msg)] safe-inc))
        ;; (cond-> (and (nil? (:mlat state)) (:is_mlat row))
        ;;   (assoc :mlat [msg
        ;;                 (.getType msg)
        ;;                 (.getDownlinkFormat msg)
        ;;                 (.getFirstField msg)
        ;;                 (seq (:data row))
        ;;                 (org.opensky.libadsb.tools/toHexString ^bytes (:data row))]))
        (cond-> (not (.checkParity msg))
          (update :num-party-errors safe-inc)))))


(defn update-hour [state row]
  (-> state
      (assoc-in [:hours [(day-of-week (:timestamp row))
                         (hour-of-day (:timestamp row))]]
                true)))


(def record-flights-interval (* 5 60 1000))
(def flight-timeout (* 10 60 1000))
(def curviness-threshold (* 10 360))


(defn record-flights
  ([state]
   (record-flights state {}))
  ([state options]
   ;;(log "recording flights")
   (let [^java.sql.Timestamp latest-ts (:latest-ts state)]
     (update
      state
      :aircraft
      (fn [aircraft]
        (reduce-kv
         (fn [m icao info]
           (let [history (:history info)]
             (if (empty? history)
               (do
                 (assoc m icao info)
                 ;;(log "Empty history")
                 )
               (let [^java.sql.Timestamp first-ts (:timestamp (first history))
                     ^java.sql.Timestamp last-ts (:timestamp (last history))
                     duration-secs (/ (ts-diff last-ts first-ts) 1000.0)]
                 (if (or (get options :final?) (>= (ts-diff latest-ts last-ts) flight-timeout))
                   (let [c (flight-curviness history)
                         d (flight-distance history)]
                     (when (and (> c curviness-threshold)
                                (> d 5.0)
                                (< d 1000.0)
                                (> (/ c d) 50.0))
                       (log "Saving flight for %s: %s-%s (%s messages, %.1f minutes %s"
                            icao
                            first-ts
                            last-ts
                            (count history)
                            (/ duration-secs 60.0)
                            (flight-info history))
                       (a/>!! flight-chan [icao history]))
                     m)
                   (assoc m icao info))))))
         {}
         aircraft))))))


(defn remove-stale-aircraft [state]
  (let [now (:latest-ts state)
        stale-icaos (reduce-kv (fn [stale icao info]
                                 (let [last-updated (statevec-last-updated (:statevec info))]
                                   (if (> (ts-diff now last-updated) (* 15 60 1000))
                                     (conj stale icao)
                                     stale)))
                               []
                               (:aircraft state))]
    (assoc state :aircraft
           (apply dissoc (:aircraft state) stale-icaos))))


(defn maybe-record-flights [state]
  (let [^java.sql.Timestamp latest-ts (:latest-ts state)
        ^java.sql.Timestamp record-flights-check-ts (::record-flights-check-ts state)]
    (cond
      ;; Haven't checked for flights yet;
      (nil? record-flights-check-ts)
      (assoc state ::record-flights-check-ts latest-ts)
      ;; Have checked for flights recently:
      (< (ts-diff latest-ts record-flights-check-ts) record-flights-interval)
      state
      ;; Time to check for flights:
      :else
      (-> state
          (assoc ::record-flights-check-ts latest-ts)
          remove-stale-aircraft
          record-flights))))


(def print-status-interval (* 24 60 60 1000))


(defn maybe-print-status [state]
  (let [^java.sql.Timestamp latest-ts (:latest-ts state)
        ^java.sql.Timestamp print-status-ts (::print-status-ts state)]
    (cond
      ;; Haven't printed status yet.
      (nil? print-status-ts)
      (assoc state ::print-status-ts latest-ts)
      ;; Have printed status recently:
      (< (ts-diff latest-ts print-status-ts) print-status-interval)
      state
      ;; Time to log status:
      :else
      (do (log "Status %-23s errors: %-3s aircraft: %-3s" latest-ts (:num-errors state) (count (:aircraft state)))
          (assoc state ::print-status-ts latest-ts)))))


(defn update-state [^ModeSDecoder decoder state row]
  (when (= (:num-rows state) 0)
    (log "Got first record"))
  (let [^bytes data (:data row)]
    (try (let [^ModeSReply msg (.decode decoder data)]
           (-> state
               (update-state-msg row msg)
               (update-state-timestamps row)
               (update :num-rows safe-inc)
               ;;(update-in [:icaos (:icao row)] safe-inc)
               (cond-> (:is_mlat row)
                 (update :num-mlats safe-inc))
               maybe-record-flights
               maybe-print-status))
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
     (let [final-state (swap! state_ #(record-flights % {:final? true}))]
       (log "Processed %s records in %.1f s (%.1f records/s)"
            (:num-rows final-state)
            (/ duration-ms 1000.0)
            (/ num-rows (/ duration-ms 1000.0)))
       final-state))))


(defmulti statevec->json (fn [kind sv icao now-secs] kind))


(defmethod statevec->json :dump1090-mutability [kind sv icao now-secs]
  (let [pos (get-statevec-value sv :pos)
        ^java.sql.Timestamp last-update (statevec-last-updated sv)
        last-update-secs (if last-update
                           (- now-secs (/ (ts->ms last-update) 1000.0))
                           100000)]
    (-> {:hex icao
         :messages (get-statevec-value sv :msg-count)
         :seen last-update-secs
         :rssi (get-statevec-value sv :rssi)}
        (cond->
            pos (assoc :lat (:lat pos))
            pos (assoc :lon (:lon pos))
            pos (assoc
                 :seen_pos
                 (- now-secs
                    (/ (ts->ms (get-statevec-value-time sv :pos)) 1000.0)))
            (:squawk sv) (assoc :squawk (get-statevec-value sv :squawk))
            (:callsign sv) (assoc :flight (get-statevec-value sv :callsign))
            (:hdg sv) (assoc :track (get-statevec-value sv :hdg))
            (:vspd sv) (assoc :vert_rate (get-statevec-value sv :vspd))
            (:spd sv) (assoc :speed (get-statevec-value sv :spd))
            (:alt sv) (assoc :altitude (get-statevec-value sv :alt))))))


;; FlightAware's dump1090 uses almost the same JSON. The main
;; differences seem to be (1) some keys have different names and (2)
;; mlat- and tis-b- sourced info is called out.

(defmethod statevec->json :dump1090-fa [kind sv icao now-secs]
  (let [json (statevec->json :dump1090-mutability sv icao now-secs)]
    (set/rename-keys
     json
     {:altitude :alt_baro
      :speed :gs
      :vert_rate :baro_rate})))


(defn aircraft->json [kind aircraft now-secs]
  (map (fn [[icao info]]
         (statevec->json kind (:statevec info) icao now-secs))
       aircraft))


(defn state->aircraft-json [kind state]
  (let [^java.sql.Timestamp latest-ts (:latest-ts state)
        now-secs (if latest-ts
                   (/ (ts->ms latest-ts) 1000.0)
                   nil)]
    {:now now-secs
     :messages (:num-rows state)
     :aircraft (aircraft->json kind (:aircraft state) now-secs)}))


(defn serve-receiver-json [state_]
  {:status 200
   :headers {"Content-Type" "application/json; charset=utf-8"}
   :body (-> @state_
             state->receiver-json
             json/generate-string)})


(defn serve-aircraft-json [kind state_]
  (let [json-gen (partial state->aircraft-json kind)]
    {:status 200
     :headers {"Content-Type" "application/json; charset=utf-8"}
     :body (-> @state_
               json-gen
               json/generate-string)}))

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
    (flight-recorder)
    (let [state_ (atom (statevec/initial-state 34.13366 -118.19241))]
      (statevec/start-server state_)
      (statevec/process-all state_ options)
      (log "Found %s distinct ICAOs" (count (:aircraft @state_)))
      ;;(pprint/pprint @state_)
      ))
  (System/exit 0))
