FROM quay.io/ceph/ceph:v20.2.0

RUN mkdir -p /var/lib/ceph/radosgw/ceph-rgw1
RUN ceph-authtool \
    --create-keyring /var/lib/ceph/radosgw/ceph-rgw1/keyring \
    --gen-key -n client.rgw1 \
    --cap osd 'allow *' \
    --cap mon 'allow *'
