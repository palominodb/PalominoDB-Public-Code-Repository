package ProcessCounts;
use strict;
use warnings FATAL => 'all';
use ProcessLog;
use Exporter;
use DBI;
use MysqlSlave;
use Statistics;
use Carp;
use FailoverPlugin;
our @ISA = qw(FailoverPlugin);

sub new {
  my $class = shift;
  my $opts = shift;
  return bless $class->SUPER::new($opts), $class;
}

sub user_count {
  my $dsn = shift;
  my $dbh = $dsn->get_dbh(1);
  # Get aggregate by user
  my $cnts = Statistics::aggsum(
    $dbh->selectall_arrayref(qq|SHOW PROCESSLIST|, { Slice => {} }),
    'User'
  );
  my $cnt_str = $dsn->get('h') . " users: ";
  foreach my $u (sort keys(%$cnts)) {
    $cnt_str .= "${u}: $cnts->{$u}, ";
  }
  chop($cnt_str); chop($cnt_str);
  $::PLOG->i($cnt_str);
}

sub pre_verification {
  my $self = shift;
  my $pridsn = $_[0];
  my $faildsn = $_[1];

  # Report on user connections and prompt to continue
  for(@_) { user_count($_); }
  if($::PLOG->p('Continue [Yes/No]?', qr/^(Yes|No)$/i) eq 'No') {
    croak('Aborting failover');
  }
}

sub post_verification {
  my $self = shift;
  my $status = shift;
  for(@_) { user_count($_); }
}

1;
