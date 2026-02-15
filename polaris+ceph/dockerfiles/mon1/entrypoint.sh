#!/bin/sh

if [ ! -f "/etc/ceph/ceph.client.admin.keyring" ]; then
    echo "Start creating keyrings (Frist time only)"

    ceph-authtool \
    --create-keyring /etc/ceph/ceph.client.admin.keyring \
    --gen-key -n client.admin \
    --cap mon 'allow *' \
    --cap osd 'allow *' \
    --cap mgr 'allow *' \
    --cap mds 'allow *'

    ceph-authtool \
    --create-keyring /etc/ceph/ceph.client.bootstrap-osd.keyring \
    --gen-key -n client.bootstrap-osd \
    --cap mon 'profile bootstrap-osd' \
    --cap mgr 'allow r'
else
    echo "Skipping to create keyrings"
fi

exec "$@"
