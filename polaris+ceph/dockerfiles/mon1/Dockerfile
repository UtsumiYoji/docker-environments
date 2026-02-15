FROM quay.io/ceph/ceph:v20.2.0

USER ceph

RUN ceph-authtool \
    --create-keyring /var/lib/ceph/tmp/ceph.mon.keyring \
    --gen-key -n mon. \
    --cap mon 'allow *'

COPY entrypoint.sh /entrypoint.sh
