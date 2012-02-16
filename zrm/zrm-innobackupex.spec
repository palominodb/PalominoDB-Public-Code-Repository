Name: zrm-innobackupex
Summary: xtrabackup copy plugin for ZRM
Version: 0.81.3
Vendor: PalominoDB
Release: 1
License: GPL
Group: Application/System
Source: http://dev.palominodb.com/src/zrm-innobackupex-%{version}.tar.gz
URL: http://blog.palominodb.com
Requires: xtrabackup >= 1.0, xinetd, perl(DBD::mysql)
Conflicts: zrm-innobackupex-client
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

%description
Provides a ZRM plugin to use xtrabackup and innobackupex to provide
true mysql hotcopy.

%prep
%setup -q

%build

%{__rm} -f examples/pre-backup.pdb.pl
%{__rm} -f examples/post-backup.pdb.pl
%{__rm} -f exmamples/socket-server.conf

%install

%{__rm} -rf %{buildroot}
%{__mkdir} -p %{buildroot}
%{__install} -b -D -m 0644 examples/xtrabackup-agent.xinetd %{buildroot}/etc/xinetd.d/xtrabackup-agent
%{__install} -b -D -m 0644 examples/socket-server.conf %{buildroot}/usr/share/mysql-zrm/plugins/socket-server.conf
%{__install} -b -d -m 0755 %{buildroot}/usr/share/mysql-zrm/plugins/
%{__install} -b -m 0755 -t %{buildroot}/usr/share/mysql-zrm/plugins/ plugins/*

%clean
%{__rm} -rf %{buildroot}

%post
if [[ -f /etc/xinetd.d/mysql-zrm-socket-server ]]; then
  %{__sed} -i -e '/disable/ s/no/yes/' /etc/xinetd.d/mysql-zrm-socket-server
fi
echo "You must restart xinetd for %{name} to take effect,"
echo "if this is the first time it's been installed."
echo ""
echo "The original ZRM socket-server has been disabled, if it was enabled."

%files
%defattr(0644,root,root)
%config /etc/xinetd.d/xtrabackup-agent
%defattr(0755,mysql,mysql)
/usr/share/mysql-zrm/plugins/stub-snapshot.pl
/usr/share/mysql-zrm/plugins/xtrabackup-client.pl
/usr/share/mysql-zrm/plugins/xtrabackup-agent.pl
