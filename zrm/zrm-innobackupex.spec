Name: zrm-innobackupex
Summary: innobackupex copy plugin for ZRM
Version: 0.72
Vendor: PalominoDB
Release: 1
License: Private
Group: Application/System
Source: http://palominodb.com/src/zrm-innobackupex-%{version}.tgz
URL: http://blog.palominodb.com
Requires: xtrabackup >= 0.9, xinetd
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

%description
Provides a ZRM plugin to use xtrabackup and innobackupex to provide
true mysql hotcopy.

%prep
%setup -q

%build

%{__rm} plugins/pre-backup.pdb.pl
%{__rm} plugins/post-backup.pdb.pl

%install

%{__rm} -rf %{buildroot}
%{__mkdir} -p %{buildroot}
%{__install} -b -D -m 0644 examples/zrm-palomino.xinetd %{buildroot}/etc/xinetd.d/zrm-palomino
%{__install} -b -D -m 0644 examples/socket-server.conf %{buildroot}/usr/share/mysql-zrm/plugins/socket-server.conf
%{__install} -b -d -m 0755 %{buildroot}/usr/share/mysql-zrm/plugins/
%{__install} -b -m 0755 -t %{buildroot}/usr/share/mysql-zrm/plugins/ plugins/*

%clean
%{__rm} -rf %{buildroot}

%post
if [[ -f /etc/xinetd.d/mysql-zrm-socket-server ]]; then
  %{__sed} -i -e '/disable/ s/no/yes/' /etc/xinetd.d/mysql-zrm-socket-server
fi
%{__echo} "You must restart xinetd for %{name} to take effect,"
%{__echo} "if this is the first time it's been installed."
%{__echo} ""
%{__echo} "The original ZRM socket-server has been disabled, if it was enabled."

%files
%defattr(0644,root,root)
/etc/xinetd.d/zrm-palomino
%defattr(0755,mysql,mysql)
%attr(0644, mysql,mysql) %config /usr/share/mysql-zrm/plugins/socket-server.conf
/usr/share/mysql-zrm/plugins/inno-snapshot.pl
/usr/share/mysql-zrm/plugins/socket-copy.palomino.pl
/usr/share/mysql-zrm/plugins/socket-server.palomino.pl
