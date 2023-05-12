(ns potamic.fmt-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.string :as string]
            [potamic.fmt :as fmt]))

(deftest LOGO-test
  (testing "potamic.fmt/LOGO"
    (is (= fmt/LOGO
"
 ___     _              _
| _ \\___| |_ __ _ _ __ (_)__
|  _/ _ \\  _/ _` | '  \\| / _|
|_| \\___/\\__\\__,_|_|_|_|_\\__|
"))
    )) ; end LOGO-test

(deftest BLUE-test
  (testing "potamic.fmt/BLUE"
    (is (= fmt/BLUE "\033[0;34m"))
    )) ; end BLUE-test

(deftest BOLD-test
  (testing "potamic.fmt/BOLD"
    (is (= fmt/BOLD "\033[1m"))
    )) ; end BOLD-test

(deftest CYAN-test
  (testing "potamic.fmt/CYAN"
    (is (= fmt/CYAN "\033[0;36m"))
    )) ; end CYAN-test

(deftest GREEN-test
  (testing "potamic.fmt/GREEN"
    (is (= fmt/GREEN "\033[0;32m"))
    )) ; end GREEN-test

(deftest ITAL-test
  (testing "potamic.fmt/ITAL"
    (is (= fmt/ITAL "\033[3m"))
    )) ; end ITAL-test

(deftest NC-test
  (testing "potamic.fmt/NC"
    (is (= fmt/NC "\033[0m"))
    )) ; end NC-test

(deftest ORANGE-test
  (testing "potamic.fmt/ORANGE"
    (is (= fmt/ORANGE "\033[0;33m"))
    )) ; end ORANGE-test

(deftest PURPLE-test
  (testing "potamic.fmt/PURPLE"
    (is (= fmt/PURPLE "\033[0;35m"))
    )) ; end PURPLE-test

(deftest RED-test
  (testing "potamic.fmt/RED"
    (is (= fmt/RED "\033[0;31m"))
    )) ; end RED-test

(deftest echo-test
  (testing "potamic.fmt/echo"
    (is (= (with-out-str (fmt/echo :debug "abc"))
           (str fmt/PURPLE "[DEBUG]" fmt/NC " abc\n")))
    (is (= (with-out-str (fmt/echo :error "abc"))
           (str fmt/RED "[ERROR]" fmt/NC " abc\n")))
    (is (= (with-out-str (fmt/echo :help "abc"))
           (str fmt/GREEN "[HELP]" fmt/NC " abc\n")))
    (is (= (with-out-str (fmt/echo :hint "abc"))
           (str fmt/CYAN "[HINT]" fmt/NC " abc\n")))
    (is (= (with-out-str (fmt/echo :info "abc"))
           (str fmt/BLUE "[INFO]" fmt/NC " abc\n")))
    (is (= (with-out-str (fmt/echo :success "abc"))
           (str fmt/GREEN "[SUCCESS]" fmt/NC " abc\n")))
    (is (= (with-out-str (fmt/echo :warn "abc"))
           (str fmt/RED "[WARN]" fmt/NC " abc\n")))
    (is (= (with-out-str (fmt/echo :warn "abc"))
           (str fmt/RED "[WARN]" fmt/NC " abc\n")))
    )) ; end echo-test

(deftest make-prefix-test
  (testing "potamic.fmt/make-prefix"
    (is (= (fmt/make-prefix :debug)   (str fmt/PURPLE "[DEBUG]"   fmt/NC)))
    (is (= (fmt/make-prefix :error)   (str fmt/RED    "[ERROR]"   fmt/NC)))
    (is (= (fmt/make-prefix :help)    (str fmt/GREEN  "[HELP]"    fmt/NC)))
    (is (= (fmt/make-prefix :hint)    (str fmt/CYAN   "[HINT]"    fmt/NC)))
    (is (= (fmt/make-prefix :info)    (str fmt/BLUE   "[INFO]"    fmt/NC)))
    (is (= (fmt/make-prefix :success) (str fmt/GREEN  "[SUCCESS]" fmt/NC)))
    (is (= (fmt/make-prefix :warn)    (str fmt/RED    "[WARN]"    fmt/NC)))
    )) ; end make-prefix-test

(deftest pretty-clj-test
  (testing "potamic.fmt/pretty-clj"
    (is (= (fmt/pretty-clj {:a 1 :b 2}) "{:a 1, :b 2}\n"))
    (is (= (fmt/pretty-clj [1 2 3]) "[1 2 3]\n"))
    )) ; end pretty-clj-test
