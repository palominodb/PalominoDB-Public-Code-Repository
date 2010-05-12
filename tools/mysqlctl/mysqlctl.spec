Name: mysqlctl
Summary: Better mysql init script
Version: 0.01
Vendor: PalominoDB
Release: 1
License: Private
Group: Application/System
Source: http://bastion.palominodb.com/releases/SRC/mysqlctl-%{version}.tar.gz
URL: http://blog.palominodb.com
Requires: bash >= 3.2, mysql-client >= 5.0
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

%description
Better init script for mysql that does
slave starting/stopping, log flushing
and has useful exit codes for automation tools.

%prep
%setup -q

%build

%install

%{__rm} -rf %{buildroot}
%{__mkdir} -p %{buildroot}
%{__install} -D -m 0755 mysqlctl %{buildroot}/%{_bindir}/mysqlctl
%{__install} -D -m 0600 sample-myctl.cnf %{buildroot}/%{_sysconfdir}/myctl.cnf

%clean

%{__rm} -rf %{buildroot}

%files
%defattr(0755,root,root)
%doc README
%config %{_sysconfdir}/myctl.cnf
%{_bindir}/mysqlctl
