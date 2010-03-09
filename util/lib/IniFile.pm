package IniFile;

# Loads a my.cnf into a hash.
# of the form:
# key: group
# val: { <option> => <value> }
# Strips spaces and newlines.
sub read_config {
  my $file = shift;
  my %cfg;
  my $inif;
  unless(open $inif, "<$file") {
    return undef;
  }
  my $cur_sec = '';
  while(<$inif>) {
    chomp;
    next if(/^\s*(?:;|#)/);
    next if(/^$/);
    if(/^\s*\[(\w+)\]/) {
      $cfg{$1} = ();
      $cur_sec = $1;
    }
    else {
      my ($k, $v) = split(/=/, $_, 2);
      $k =~ s/\s+$//;
      if(defined($v)) {
        $v =~ s/^\s+//;
      }
      else {
        $v = 1;
      }
      chomp($k); chomp($v);
      $cfg{$cur_sec}{$k} = $v;
    }
  }
  return %cfg;
}

1;
