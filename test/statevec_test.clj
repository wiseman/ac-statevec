(ns statevec-test
  (:require [statevec :as statevec]
            [clojure.test :as t]))

(def epsilon 0.00001)

(defn a= [^double a ^double b]
  (< (Math/abs (- a b)) epsilon))


(t/testing "Utilities"
  (t/deftest dbfs
    (t/is (a= 0.0 (statevec/dbfs 255)))
    (t/is (a= -54.15140 (statevec/dbfs 0)))
    (t/is (a= -5.98660 (statevec/dbfs 128)))))

(t/testing "State vector utilities"
  (t/deftest state-vector-utilities
    (let [sv (statevec/update-state-vector-value {} :speed 1.0 100)]
      (t/is (= 1.0 (statevec/get-state-vector-value sv :speed)))
      (t/is (= 100 (statevec/get-state-vector-value-time sv :speed)))
      )))



(def yosemite {:lat 34.13376 :lon -118.19245})
(def lax {:lat 33.94158 :lon -118.40853})

(t/testing "geo utils"
  (t/deftest bearing
    (t/is (a= 315.00436 (statevec/bearing {:lat 0 :lon 0} {:lat 1 :lon -1})))
    (t/is (a= 223.03631 (statevec/bearing yosemite lax)))
    (t/is (a= 42.91536 (statevec/bearing lax yosemite))))
  (t/deftest distance
    (t/is (a= 157.29381 (statevec/distance {:lat 0 :lon 0} {:lat 1 :lon -1})))
    (t/is (a= 29.21574 (statevec/distance yosemite lax)))
    (t/is (a= 29.21574 (statevec/distance lax yosemite)))))


;; (defn make-circle [clat clon rad t]
;;   (range 0 (Math/PI

;; (t/testing "curviness")
