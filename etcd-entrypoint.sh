#!/bin/sh
# Generates the goreman Procfile at runtime using ADVERTISE_IP so that
# remote clients (Machines 2 & 3) receive the correct advertise-client-url
# instead of 127.0.0.1 (which is unreachable from other machines).
ADVERTISE_IP="${ADVERTISE_IP:-127.0.0.1}"

# Wipe stale data so --initial-cluster-state new always works on restart
rm -rf /data/node1/* /data/node2/* /data/node3/*

cat > /Procfile << EOF
node1: etcd --name node1 --data-dir /data/node1 --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://${ADVERTISE_IP}:2379 --listen-peer-urls http://0.0.0.0:2380 --initial-advertise-peer-urls http://127.0.0.1:2380 --initial-cluster node1=http://127.0.0.1:2380,node2=http://127.0.0.1:2382,node3=http://127.0.0.1:2384 --initial-cluster-token etcd-cluster-1 --initial-cluster-state new
node2: etcd --name node2 --data-dir /data/node2 --listen-client-urls http://0.0.0.0:2381 --advertise-client-urls http://${ADVERTISE_IP}:2381 --listen-peer-urls http://0.0.0.0:2382 --initial-advertise-peer-urls http://127.0.0.1:2382 --initial-cluster node1=http://127.0.0.1:2380,node2=http://127.0.0.1:2382,node3=http://127.0.0.1:2384 --initial-cluster-token etcd-cluster-1 --initial-cluster-state new
node3: etcd --name node3 --data-dir /data/node3 --listen-client-urls http://0.0.0.0:2383 --advertise-client-urls http://${ADVERTISE_IP}:2383 --listen-peer-urls http://0.0.0.0:2384 --initial-advertise-peer-urls http://127.0.0.1:2384 --initial-cluster node1=http://127.0.0.1:2380,node2=http://127.0.0.1:2382,node3=http://127.0.0.1:2384 --initial-cluster-token etcd-cluster-1 --initial-cluster-state new
EOF

exec goreman -f /Procfile start
