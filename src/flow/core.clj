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

(defn- agent-queue [fn state]
  (agent {:fn fn
          :state state}))

(defn- agent-queue-dispatch
  [{fn :fn
    state :state
    :as old} & args]
  (apply fn old args))

(defn flow-value
  ([] (flow-value nil))
  ([init]
     (let [f (fn [old this port value graph]
               (distribute graph this :out value)
               (assoc old :state value))
           a (agent-queue f init)]
       (reify
         IFlowNode
         (queue [this port value graph]
           (when (= port :in)
             (send a agent-queue-dispatch this port value graph)))
         IDeref
         (deref [this]
           (:state @a))))))

(defn filter-node
  [f]
  (let [fnc (fn [old this port value graph]
              (when (f value)
                (distribute graph this :out value))
              old)
        a (agent-queue fnc nil)]
    (reify
      IFlowNode
      (queue [this port value graph]
        (when (= port :in)
          (send a agent-queue-dispatch this port value graph))))))

(defn fn-wrap-node
  [f]
  (let [fnc (fn [old this port value graph]
              (distribute graph this :out (f value))
              old)
        a (agent-queue fnc nil)]
    (reify
      IFlowNode
      (queue [this port value graph]
        (when (= port :in)
          (send a agent-queue-dispatch this port value graph))))))


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
           (dorun
              (for [[nd pt] (connections @g node port)]
                   (queue nd pt value this))))
         (-alter-graph! [this f args]
           (apply swap! g f args))
         (graph [this]
           @g)
         (inject [this node port value]
           (queue node port value this))))))
