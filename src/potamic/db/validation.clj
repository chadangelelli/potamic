(ns potamic.db.validation
  (:require [malli.core :as malli]
            [potamic.validation :as v])
  (:gen-class))

(def Redis-Spec
  [:map {:closed true}
   [:uri (v/f v/valid-redis-uri? "Invalid Redis URI")]])

(def Kvrocks-Spec
  [:map {:closed true}
   [:host [:string {:min 1}]]
   [:port :int]
   [:password [:string {:min 1}]]
   [:db :int]])

(def Valid-Conn
  (malli/schema
    [:map {:closed true}
     [:backend {:optional true} [:enum :redis :kvrocks]]
     [:spec [:or Redis-Spec Kvrocks-Spec]]
     [:pool {:optional true} map?]]))

(def Valid-Make-Conn-Args
  [:or
   [:map {:closed true}
    [:backend {:optional true} [:= :redis]]
    [:uri (v/f v/valid-redis-uri? "Invalid Redis URI")]
    [:pool {:optional true} map?]]
   [:map {:closed true}
    [:backend [:= :kvrocks]]
    [:host [:string {:min 1}]]
    [:port [:or :int [:re "[1-9][0-9]+"]]]
    [:password [:string {:min 1}]]
    [:db :int]
    [:pool {:optional true} map?]]])
