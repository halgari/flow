(ns flow.core
  (:import [clojure.lang IDeref]))

(defprotocol IConnectionManager
  (connect [this src dest]
           [this src srcpt dest destpt])
  (connected? [this src srcport dest])
  (connections [this src port])
  (add-node [this node]))

(defrecord DataGraph [name connections nodes]
  IConnectionManager
  (connect [this src dest]
    (update-in this [:connections src] (fnil conj #{}) dest))
  (connect [this src srcpt dest destpt]
                (connect this [src srcpt] [dest destpt]))

  (connected? [this src srcport dest]
    (some #(= (first %) dest)
          (get-in this [:connections [src srcport]])))
  (connections [this src port]
    (get-in this [:connections [src port]]))
  (add-node [this node]
    (let [nk (keyword (name (gensym "dataflow_node")))]
      (assoc-in this [:nodes nk] node))))


(defprotocol IFlowNode
  (queue [this port value graph]))

(defprotocol IDistributor
  (distribute [this node port value])
  (-alter-graph! [this f args])
  (graph [this])
  (inject [this node port value]))

(defn- second-arg [a b] b)

(defn flow-value
  ([] (flow-value nil))
  ([init]
     (let [a (atom init)]
       (reify
         IFlowNode
         (queue [this port value graph]
           (when (= port :in)
             (swap! a second-arg value)
             (distribute graph this :out value)))
         IDeref
         (deref [this]
           @a)))))

(defn fn-wrap-node
  [f]
  (reify
    IFlowNode
    (queue [this port value graph]
      (when (= port :in)
        (distribute graph this :out (f value))))))


(defn datagraph
  ([] (datagraph nil))
  ([name]
     (DataGraph. name {} {})))

(defn alter-graph! [this f & args]
  (-alter-graph! this f args))

(defn simple-distributor
  "Constructs a simple distributor that processes nodes via agents"
  ([] (simple-distributor (datagraph)))
  ([graph]
     (let [a (atom {})
           g (atom graph)]
       (reify
         IDistributor
         (distribute [this node port value]
           (future (dorun
                    (for [[nd pt] (connections @g node port)]
                      (queue nd pt value this)))))
         (-alter-graph! [this f args]
           (apply swap! g f args))
         (graph [this]
           @g)
         (inject [this node port value]
           (queue node port value this))))))
