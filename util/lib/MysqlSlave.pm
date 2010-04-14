package MysqlSlave;
use strict;
use warnings FATAL => 'all';
use Carp;

sub new {
  my ($class, $dsn) = @_;
  my $self = {};
  $self->{dsn} = $dsn;
  return bless $self, $class;
}

sub read_only {
  my ($self, $value) = @_;
  my $dbh = $self->{dsn}->get_dbh(1);
  if(defined($value)) {
    croak('value must be 0 or 1') unless( $value eq '0' or $value eq '1' );
    $dbh->do('SET GLOBAL read_only = '. int($value));
  }
  return $dbh->selectcol_arrayref('SELECT @@read_only')->[0];
}

sub auto_inc_inc {
  my ($self) = @_;
  my $dbh = $self->{dsn}->get_dbh(1);
  return $dbh->selectcol_arrayref('SELECT @@auto_increment_increment')->[0];
}

sub auto_inc_off {
  my ($self) = @_;
  my $dbh = $self->{dsn}->get_dbh(1);
  return $dbh->selectcol_arrayref('SELECT @@auto_increment_offset')->[0];
}

sub master_status {
  my ($self) = @_;
  my $dbh = $self->{dsn}->get_dbh(1);
  my ($log_file, $log_pos) = $dbh->selectrow_array('SHOW MASTER STATUS');

  return wantarray ? ($log_file, $log_pos) : $log_file ? 1 : 0;
}

sub slave_status {
  my ($self) = @_;
  my $dbh = $self->{dsn}->get_dbh(1);
  return $dbh->selectrow_hashref(q|SHOW SLAVE STATUS|);
}

sub flush_logs {
  my ($self) = @_;
  my $dbh = $self->{dsn}->get_dbh(1);
  return $dbh->do('FLUSH LOGS');
}

sub start_slave {
  my ($self, $master_log_file, $master_log_pos) = @_;
  my $dbh = $self->{dsn}->get_dbh(1);
  if($master_log_file and $master_log_pos) {
    $master_log_file = $dbh->quote($master_log_file);
    return $dbh->do("START SLAVE UNTIL MASTER_LOG_FILE=$master_log_file, MASTER_LOG_POS=" . int($master_log_pos));
  }
  return $dbh->do('START SLAVE');
}

sub stop_slave {
  my ($self) = @_;
  my $dbh = $self->{dsn}->get_dbh(1);
  return $dbh->do('STOP SLAVE');
}

sub change_master_to {
  my ($self, @args) = @_;
  my $dbh = $self->{dsn}->get_dbh(1);
  my %master_keys = (
    MASTER_HOST => 1,
    MASTER_USER => 1,
    MASTER_PASSWORD => 1,
    MASTER_PORT => 1,
    MASTER_LOG_FILE => 1,
    MASTER_LOG_POS => 1,
    MASTER_SSL => 1,
    MASTER_SSL_CA => 1,
    MASTER_SSL_CAPATH => 1,
    MASTER_SSL_CERT => 1,
    MASTER_SSL_KEY => 1,
    MASTER_SSL_CIPHER => 1,
    MASTER_CONNECT_RETRY => 1,
    MASTER_SSL_VERIFY_SERVER_CERT => 1
  );
  my $sql = 'CHANGE MASTER TO ';
  my %keys = ();
  if(ref($args[0])) {
    %keys = %{$args[0]};
  }
  else {
    %keys = @args;
  }
  for(keys %keys) {
    croak("Invalid option $_") unless( exists($master_keys{uc($_)}) );
    if($keys{$_} =~ /^\d+$/) {
      $sql .= uc($_) . '=' . $keys{$_} . ', ';
    }
    else {
      $sql .= uc($_) . '=' . $dbh->quote($keys{$_}) . ', ';
    }
  }
  chop($sql);
  chop($sql);
  return $dbh->do($sql);
}

=pod

=head1 NAME

MysqlSlave - Deal with MySQL Slaves and Masters

=head1 RISKS

This module performs many administrative tasks. Some of the
things it is able to do are very destructive. You C<can> easily
lose data with improper usage of this module.

=head1 SYNOPSIS

This module provides simple functions for dealing with MySQL slave
and master hosts.


  my $slave = MysqlSlave->new($dsn);

  $slave->read_only;    # Gets the value of the 'read_only' variable
  $slave->read_only(1); # Sets the value of the 'read_only' variable

  $slave->stop_slave;   # *Actually* stops the slave
  $slave->start_slave;  # *Actually* starts the slave

  # Get master binlog information
  my ($log_file, $log_pos) = $slave->master_status;

  # Start slave UNTIL $master_log_file, $master_log_pos
  $slave->start_slave($master_log_file, $master_log_pos);

  # Change master host
  $slave->change_master_to(
    master_host     => $host,
    master_log_file => $logfile,
    master_log_pos  => $logpos
  );

  $slave->flush_logs; # *Actually* flush logs

=head1 METHODS

=over 8

=item C<new($dsn)>

Creates a new MysqlSlave object from C<$dsn> .

=item C<read_only([$set])>

Sets/Gets C<read_only>. Requires C<SUPER> privilege to set.
Croaks with C<"Set read_only needs SUPER">, if user does not have C<SUPER>.

=item C<auto_inc_inc()>

Gets the slave's C<@@auto_increment_increment> variable.
No facility is provided to change this value.

=item C<auto_inc_off()>

Gets the slave's C<@@auto_increment_offset> variable.
No facility is provided to change this value.

=item C<start_slave([$master_log_file, $master_log_pos])>

Starts the slave threads. Requires C<SUPER> privilege.
Croaks with C<"Start slave needs SUPER">, if user does not have C<SUPER>.

When C<$master_log_file> and $C<$master_log_pos> are set, then this
will execute a C<START SLAVE UNTIL MASTER_LOG_FILE='????', MASTER_LOG_POS=????>.
There is not currently support for starting until a relay log position.

=item C<stop_slave()>

Starts the slave threads. Requires C<SUPER> privilege.
Croaks with C<"Start slave needs SUPER">, if user does not have C<SUPER>.

=item C<master_status()>

In scalar context returns true if the host has master logs, i.e. looks like a master and false otherwise.
In array/list context returns the master log file and master log position in that order.

=item C<flush_logs()>

Executes: C<FLUSH LOGS> which will (among other things) flush the binary log
and cause a rotation. Requires C<SUPER> privilege.
Croaks with C<"Flush logs needs SUPER"> if user does not have C<SUPER>.

=item C<change_master_to(...)>

Takes a hash or hashref of C<CHANGE MASTER TO ...> options.
From L<http://dev.mysql.com/doc/refman/5.1/en/change-master-to.html> those are:

=over 8

=item * MASTER_HOST
=item * MASTER_USER
=item * MASTER_PASSWORD
=item * MASTER_PORT
=item * MASTER_LOG_FILE
=item * MASTER_LOG_POS
=item * MASTER_SSL
=item * MASTER_SSL_CA
=item * MASTER_SSL_CAPATH
=item * MASTER_SSL_CERT
=item * MASTER_SSL_KEY
=item * MASTER_SSL_CIPHER
=item * MASTER_CONNECT_RETRY
=item * MASTER_SSL_VERIFY_SERVER_CERT (5.1 only)

=back

Croaks with C<"Invalid option XXX"> if an invalid option is specified.
Where C<XXX> is the invalid option.

The values are not case-sensitive.
If the server is 5.0 and a 5.1 option is passed, it will be silently discarded.

=back

=cut

1;
