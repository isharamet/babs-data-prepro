(ns babs-data-prepro.core
  (:require [clojure.string :as s]
            [me.raynes.fs :as fs]
            [clj-time.format :as f]
            [clojure.string :as str])
  (:gen-class))

(def default-formatter (f/formatter "yyyy-MM-dd"))

(def files
  [{:file-mask #".+_station_data.csv" :file-type "station-data" :date-pos 6 :date-format "MM/dd/yyyy"}
   {:file-mask #".+_status_data.csv" :file-type "status-data" :date-pos 3 :date-format "yyyy-MM-dd HH:mm:ss"}
   {:file-mask #".+_trip_data.csv" :file-type "trip-data" :date-pos 2 :date-format "yyyy/MM/dd HH:mm:ss"}
   {:file-mask #".+_weather_data.csv" :file-type "weather-data" :date-pos 0 :date-format "MM/dd/yyyy"}])

(defn write-file
  [lines file-type file-date]
  (let [dir (str "resources/" file-type)
        file (str dir "/" file-date ".csv")]
    (do
      (if (not (fs/exists? dir))
        (fs/mkdir dir))
      (with-open [wrtr (clojure.java.io/writer file :append true)]
        (doseq [line lines]
          (.write wrtr line)
          (.newLine wrtr))))))

(defn strip-quotes
  [quoted]
  (s/replace quoted "\"" ""))

(defn process-file
  [file-path file-info]
  (let [{:keys [file-type date-pos date-format]} file-info
        formatter (f/formatter date-format)]
    (with-open [rdr (clojure.java.io/reader file-path)]
      (let [res (reduce
                  (fn [acc line]
                    (let [lines (:lines acc)
                          prev-date (:prev-date acc)
                          xs (s/split line #",")
                          raw-date (strip-quotes (nth xs date-pos))
                          date (f/unparse default-formatter (f/parse formatter raw-date))]
                      (if (and (not (nil? prev-date)) (not= prev-date date))
                        (do
                          (write-file lines file-type prev-date)
                          {:lines [line] :prev-date date})
                        {:lines (conj lines line) :prev-date date})))
                  {:lines [] :prev-date nil}
                  (rest (line-seq rdr)))]
        (write-file (:lines res) file-type (:prev-date))))))

(defn walk-files
  [dir]
  (doseq [file (fs/list-dir dir)
          file-info files]
    (if (re-matches (:file-mask file-info) (.getName file))
      (process-file (.getPath file) file-info))))

(defn -main
  [dir & args]
  (walk-files dir))