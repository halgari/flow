(ns flow.core_spec
  (:use [speclj.core]
        [flow.core])
  (:import [clojure.lang PersistentQueue IDeref]))

(defn mock-distributor
  "Creates a fake distributor that simply holds a queue of pending
   distributions."
  []
  (let [a (atom [])
        graph (atom (datagraph))]
    (reify
      IDeref
      (deref [this] @a)
      IDistributor
      (distribute [this nodename port value]
        (swap! a conj [nodename port value]))
      (-alter-graph! [this f args]
        (apply swap! graph f args)))))

(defn debug [x]
  (print x)
  x)


(describe "datagraph"
          (it "can be creatable"
              (should (datagraph)))
          (it "can have a name"
              (should (datagraph "Main Graph")))
          (it "can connect nodes"
              (let [graph (connect (datagraph) :src :out :dest :in)]
                (should (connected? graph :src :out :dest))
                (should-not (connected? graph :src :out :bar))))
          (it "can return connectors"
              (let [graph (connect (datagraph) :src :out :dest :in)]
                (should (= (connections graph :src :out)
                           #{[:dest :in]})))))

(describe "flow-value"
          (it "can be created"
              (should (flow-value)))
          (it "takes an optional init argument"
              (should (flow-value 42)))
          (it "can be deref'ed"
              (should (= @(flow-value 42) 42)))
          (it "can accept values"
              (should (queue (flow-value)
                             :in
                             42
                             (mock-distributor))))
          (it "calls distribute"
              (let [d (mock-distributor)
                    v (flow-value)]
                (queue v :in 42 d)
                (should (= @d [[v :out 42]])))))

(describe "simple-distributor"
          (it "can be created"
              (should (simple-distributor)))
          (it "accepts a default dataflow"
              (should (simple-distributor (datagraph))))
          (it "alters graphs"
              (let [d (simple-distributor)]
                (alter-graph! d connect :src :out :dest :in)
                (should (connected? (graph d) :src :out :dest))))
          (it "allows injection"
              (let [d (simple-distributor)
                    fv1 (flow-value)
                    fv2 (flow-value)]
                (alter-graph! d connect fv1 :out fv2 :in)
                (should (connected? (graph d) fv1 :out fv2))
                (inject d fv1 :in 42)
                (should (= @fv2 42)))))

(run-specs)
