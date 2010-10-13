class yumrepos::default {
  yumrepo { 'rpmforge':
    mirrorlist => 'http://apt.sw.be/redhat/el5/en/mirrors-rpmforge',
    enabled    => 1,
    gpgcheck   => 0,
  }
}
