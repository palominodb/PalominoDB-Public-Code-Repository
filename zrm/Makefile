.PHONY: all
all:
	echo Nothing to make.

install:
	install -d $(DESTDIR)/usr/share/mysql-zrm/plugins
	install -d $(DESTDIR)/var/log/mysql-zrm/
	install -d $(DESTDIR)/usr/share/zrm-innobackupex/examples
	install -m 0755 plugins/xtrabackup-agent.pl $(DESTDIR)/usr/share/mysql-zrm/plugins/xtrabackup-agent.pl
	install -m 0755 plugins/xtrabackup-client.pl $(DESTDIR)/usr/share/mysql-zrm/plugins/xtrabackup-client.pl
	install -m 0755 plugins/stub-snapshot.pl $(DESTDIR)/usr/share/mysql-zrm/plugins/stub-snapshot.pl
	install -D -m 0644 examples/xtrabackup-agent.xinetd $(DESTDIR)/etc/xinetd.d/xtrabackup-agent
	install examples/* $(DESTDIR)/usr/share/zrm-innobackupex/examples/
