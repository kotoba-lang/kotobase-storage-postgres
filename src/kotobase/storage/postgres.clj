(ns kotobase.storage.postgres
  "PostgreSQL implementation of immutable blocks and linearizable refs."
  (:require [clojure.string :as str]
            [kotobase.storage.core :as storage])
  (:import [java.sql Connection PreparedStatement ResultSet]
           [java.util Arrays]
           [javax.sql DataSource]))

(def schema-statements
  ["CREATE TABLE IF NOT EXISTS kotobase_blocks (
      cid TEXT PRIMARY KEY, bytes BYTEA NOT NULL, byte_length BIGINT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now())"
   "CREATE TABLE IF NOT EXISTS kotobase_refs (
      name TEXT PRIMARY KEY, cid TEXT NOT NULL, revision BIGINT NOT NULL DEFAULT 1,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now())"])

(defn- bind! [^PreparedStatement statement params]
  (doseq [[index value] (map-indexed vector params)]
    (if (bytes? value)
      (.setBytes statement (inc index) value)
      (.setObject statement (inc index) value)))
  statement)

(defn- execute! [^Connection connection sql params]
  (with-open [statement (bind! (.prepareStatement connection sql) params)]
    (.executeUpdate statement)))

(defn- rows [^Connection connection sql params row-fn]
  (with-open [statement (bind! (.prepareStatement connection sql) params)
              result (.executeQuery statement)]
    (loop [out []]
      (if (.next result)
        (recur (conj out (row-fn result)))
        out))))

(defn initialize! [^DataSource datasource]
  (with-open [connection (.getConnection datasource)]
    (doseq [statement schema-statements]
      (execute! connection statement [])))
  datasource)

(defrecord PostgreSQLStorage [^DataSource datasource]
  storage/IBlockStore
  (-put-blocks! [_ blocks]
    (with-open [connection (.getConnection datasource)]
      (let [auto (.getAutoCommit connection)]
        (try
          (.setAutoCommit connection false)
          (doseq [{:keys [cid bytes]} blocks]
            (execute! connection
                      "INSERT INTO kotobase_blocks(cid, bytes, byte_length)
                       VALUES (?, ?, ?) ON CONFLICT (cid) DO NOTHING"
                      [cid bytes (alength bytes)]))
          (doseq [{:keys [cid bytes]} blocks
                  :let [stored
                        (first
                         (rows connection
                               "SELECT bytes FROM kotobase_blocks WHERE cid = ?"
                               [cid]
                               #(.getBytes ^ResultSet % 1)))]
                  :when (not (Arrays/equals ^bytes bytes ^bytes stored))]
            (throw
             (ex-info "CID already has different bytes"
                      {:type :kotobase.storage/cid-collision :cid cid})))
          (.commit connection)
          (mapv :cid blocks)
          (catch Throwable error
            (.rollback connection)
            (throw error))
          (finally (.setAutoCommit connection auto))))))
  (-get-blocks [_ cids]
    (if (empty? cids)
      {}
      (with-open [connection (.getConnection datasource)]
        (let [placeholders (str/join "," (repeat (count cids) "?"))]
          (into {}
                (rows connection
                      (str "SELECT cid, bytes FROM kotobase_blocks WHERE cid IN ("
                           placeholders ")")
                      cids
                      (fn [^ResultSet result]
                        [(.getString result 1) (.getBytes result 2)])))))))

  storage/IRefStore
  (-read-ref [_ name]
    (with-open [connection (.getConnection datasource)]
      (first
       (rows connection
             "SELECT cid, revision FROM kotobase_refs WHERE name = ?"
             [name]
             (fn [^ResultSet result]
               {:cid (.getString result 1)
                :version (.getLong result 2)})))))
  (-compare-and-set-ref! [this name expected next]
    (with-open [connection (.getConnection datasource)]
      (let [published
            (first
             (rows
              connection
              (if (nil? expected)
                "INSERT INTO kotobase_refs(name, cid) VALUES (?, ?)
                 ON CONFLICT (name) DO NOTHING RETURNING cid, revision"
                "UPDATE kotobase_refs
                 SET cid = ?, revision = revision + 1, updated_at = now()
                 WHERE name = ? AND cid = ? RETURNING cid, revision")
              (if (nil? expected)
                [name next]
                [next name expected])
              (fn [^ResultSet result]
                {:published? true
                 :current (.getString result 1)
                 :version (.getLong result 2)})))]
        (or published
            (let [current (storage/-read-ref this name)]
              {:published? false :current (:cid current)
               :version (:version current)})))))

  storage/IBackendCapabilities
  (-capabilities [_]
    #{:immutable-blocks :cid-addressed-read :conditional-ref
      :linearizable-ref :batch-get :batch-put :server-transaction}))

(defn open
  [{:keys [datasource initialize?] :or {initialize? true}}]
  (when-not (instance? DataSource datasource)
    (throw (ex-info "PostgreSQL storage requires javax.sql.DataSource"
                    {:type :kotobase.storage/invalid-configuration
                     :backend :postgresql})))
  (when initialize? (initialize! datasource))
  (->PostgreSQLStorage datasource))
