package Pdb::SemanticsError;
use strict;
use warnings FATAL => 'all';
use base qw(Error);
use overload ('""' => 'stringify');

use constant Unknown => 'Unknown';
use constant UnknownCluster => 'UnknownCluster';
use constant ClusterMismatch => 'ClusterMismatch';
use constant EmptyDSN => 'EmptyDSN';
use constant PrimaryMismatch => 'PrimaryMismatch';
use constant FailoverMismatch => 'FailoverMismatch';

sub new {
  my ($class, $text, $errcode) = @_;
  my @args = ();
  local $Error::Depth = $Error::Depth +1;
  local $Error::Debug = 1;

  $class->SUPER::new(-text => $text, -value => $errcode, @args);
}
1;
