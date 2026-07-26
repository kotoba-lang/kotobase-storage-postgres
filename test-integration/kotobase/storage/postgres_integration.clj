(ns kotobase.storage.postgres-integration
  (:require [kotobase.storage.contract :as contract]
            [kotobase.storage.postgres :as postgres])
  (:import [org.postgresql.ds PGSimpleDataSource]))

(defn -main [& _]
  (let [datasource (doto (PGSimpleDataSource.)
                     (.setURL (or (System/getenv "KOTOBASE_POSTGRES_URL")
                                  "jdbc:postgresql://127.0.0.1:55432/kotobase"))
                     (.setUser "kotobase")
                     (.setPassword "kotobase"))
        backend (postgres/open {:datasource datasource})
        checks (atom 0)]
    (with-open [connection (.getConnection datasource)
                statement (.createStatement connection)]
      (.execute statement
                "TRUNCATE kotobase_refs, kotobase_blocks"))
    (contract/verify
     backend
     (fn [ok? label]
       (swap! checks inc)
       (when-not ok? (throw (ex-info label {})))))
    (println (str "PostgreSQL contract: " @checks " checks"))))
