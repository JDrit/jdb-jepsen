(ns jdb-jepsen.core
    (:require [clojure.core           :as core]
              [clojure.core.reducers  :as r]
              [clojure.string         :as str]
              [clojure.java.io        :as io]
              [clj-http.client        :as http]
              [clj-http.util          :as http.util]
              [cheshire.core          :as json]
              [slingshot.slingshot    :refer [try+ throw+]])
    (:import (com.fasterxml.jackson.core JsonParseException)
             (java.io InputStream)
             (clojure.lang MapEntry)))

(def default-timeout "milliseconds" 1000)

(defn connect
  "Creates a new jdb client for the given server URI. Example:

  (def raft (connect \"http://127.0.0.1:6001\" \"client-id\"))

  Options:
  :timeout   How long, in ms, to wait for requests
  "
  ([server-uri client-id]
   (connect server-uri client-id {}))
  ([server-uri client-id opts]
   (merge {:timeout      default-timeout
           :endpoint     server-uri
           :client       client-id
           :id           (atom 0)}
        opts)))

(defn get-id [client]
  (reset! (:id client) (inc @(:id client))))

(defn base-url
  "Constructs the base url for all jdb requests. Example:

  (base-url client) ; => \"http://127.0.0.1:6001/\""
  [client]
  (str (:endpoint client) ""))

(defn parse-json
    "Parse an inputstream or string as JSON"
    [str-or-stream]
    (if (instance? InputStream str-or-stream)
          (json/parse-stream (io/reader str-or-stream) true)
          (json/parse-string str-or-stream true)))

(defn ^String encode-key-seq
  "Return a url-encoded key string for a key sequence."
  [key-seq]
  (str/join "/" (map http.util/url-encode key-seq)))

(defn ^String url
  "The URL for a key under a specified root-key.

  (url client [\"key\" \"foo\"]) ; => \"http://127.0.0.1:6001/key/foo"
  [client key-seq]
  (str (base-url client) "/" (encode-key-seq key-seq)))

(defn parse-json
  "Parse an inputstream or string as JSON"
  [str-or-stream]
  (if (instance? InputStream str-or-stream)
    (json/parse-stream (io/reader str-or-stream) true)
    (json/parse-string str-or-stream true)))

(defn parse-resp
  "Takes a clj-http response, extracts the body, and assoc's status and 
  on the response's body."
  [response]
  (when-not (:body response)
    (throw+ {:type     ::missing-body
             :response response}))

  (try+
    (let [body (parse-json (:body response))]
      (with-meta body {:status           (:status response)}))
    (catch JsonParseException e
      (throw+ {:type     ::invalid-json-response
               :response response}))))

(defmacro parse
  "Parses regular responses using parse-resp, but also rewrites slingshot
  exceptions to have a little more useful structure; bringing the json error
  response up to the top level and merging in the http :status."
  [expr]
  `(try+
     (let [r# (parse-resp ~expr)]
       r#)
     (catch (and (:body ~'%) (:status ~'%)) {:keys [:body :status] :as e#}
       ; etcd is quite helpful with its error messages, so we just use the body
       ; as JSON if possible.
       (try (let [body# (parse-json ~'body)]
              (throw+ (cond (string? body#) {:message body# :status ~'status}
                            (map? body#) (assoc body# :status ~'status)
                            :else {:body body# :status ~'status})))
            (catch JsonParseException _#
              (throw+ e#))))))

(defn http-opts
  "Given a map of options for a request, constructs a clj-http options map.
  :timeout is used for the socket and connection timeout. Remaining options are
  passed as query params."
  [client opts]
  {:as                    :string
   :throw-exceptions?     true
   :throw-entire-message? true
   :follow-redirects      true
   :force-redirects       true ; Etcd uses 307 for side effects like PUT
   :socket-timeout        (or (:timeout opts) (:timeout client))
   :conn-timeout          (or (:timeout opts) (:timeout client))
   :query-params          (dissoc opts :timeout :root-key)})

(defn get*
  "Gets the value for the given key"
  ([client key]
   (get* client key {}))
  ([client key opts]
   (->> opts
        (http-opts client)
        (http/get (url client ["get"]) {:query-params {"client" (:client client) "id" (get-id client) "key" key }})
        :body)))

(defn get
  ([client key]
   (get client key {}))
 ([client key opts] 
   (-> (get* client key opts)
       (json/parse-string true)
       :value)))

(defn put!
  "Puts a new value for the given key"
  ([client key value]
   (put! client key value {}))
  ([client key value opts]
   (->> opts
        (http-opts client)
        (http/get (url client ["put"]) {:query-params {"client" (:client client) "key" key "value" value "id" (get-id client) }})
        parse)))

(defn delete!
  "Deletes the given key from the raft cluster"
  ([client key]
   (delete! client key {}))
  ([client key opts]
   (->> opts
        (http-opts client)
        (http/get (url client ["delete"]) {:query-params {"client" (:client client) "id" (get-id client) "key" key }})
        parse)))

(defn cas*
  "CAS operation that is sent to the server"
  ([client key currentValue newValue]
   (cas* client key currentValue newValue {}))
  ([client key currentValue newValue opts]
   (->> opts
        (http-opts client)
        (http/get (url client ["cas"]) {:query-params {"client" (:client client) "id" (get-id client) "key" key "current" currentValue "new" newValue }})
        :body)))

(defn cas!
  ([client key currentValue newValue]
   (cas! client key currentValue newValue {}))
  ([client key currentValue newValue opts]
   (-> (cas* client key currentValue newValue opts)
       (json/parse-string true)
       :replaced)))

(defn append!
  "Sends an append request to the server"
  ([client key value ]
   (append! client key value {}))
  ([client key value opts]
   (->> opts
        (http-opts client)
        (http/get (url client ["append"]) {:query-params {"client" (:client client) "id" (get-id client) "key" key "value" value }})
        parse)))













