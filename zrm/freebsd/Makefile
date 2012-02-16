# New ports collection makefile for:   zrm-innobackupex
# Date created:        08 Dec 2009
# Whom:                brian
#
# $FreeBSD$
#

PORTNAME =      zrm-innobackupex
PORTVERSION =   0.75
DISTFILES =     ${PORTNAME}-${PORTVERSION}.tgz
CATEGORIES =    sysutils
MASTER_SITES =  http://bastion.palominodb.com/src/

MAINTAINER =    brian@palominodb.com
COMMENT =       InnobackupEX support for ZRM

PREFIX   = /usr

MAKEFILE = freebsd/Makefile.build
PLIST    = freebsd/pkg-plist
DESCR    = freebsd/pkg-descr
EXTRA_PKG_ARGS = -r freebsd/requirements -I freebsd/post-install -K freebsd/post-deinstall

do-post-install:
	freebsd/post-install

do-requirements:
	freebsd/requirements

do-post-deinstall:
	freebsd/post-deinstall

.include <bsd.port.mk>
