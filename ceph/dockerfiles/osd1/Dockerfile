FROM quay.io/ceph/ceph:v20.2.0

USER ceph

RUN mkdir -p /var/lib/ceph/osd/ceph-0

RUN ceph-authtool \
    --create-keyring /var/lib/ceph/osd/ceph-0/keyring \
    --gen-key -n osd.0 \
    --cap osd 'allow *' \
    --cap mon 'allow profile osd'
