package IniFile;
use strict;
use warnings FATAL => 'all';
use File::Glob;

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
    if(/^\s*\[(\w+)\]/) { # Group statement
      $cfg{$1} = {};
      $cur_sec = $1;
    }
    elsif(/^!(include(?:dir)?)\s+([^\0]+)/) { # include directives
      my $path = $2;
      my @files;
      if($1 eq 'includedir') {
        @files = glob($path . "/*.cnf");
      }
      else {
        @files = ($path);
      }
      for(@files) { _merge(\%cfg, {read_config($_)}); }
    }
    else { # options and flags
      my ($k, $v) = split(/=/, $_, 2);
      $k =~ s/\s+$//;
      if(defined($v)) {
        $v =~ s/^\s+//;
        $v =~ s/\s?#.*?[^"']$//;
        $v =~ s/^(?:"|')//;
        $v =~ s/(?:"|')$//;
      }
      else {
        if($k =~ /^(?:no-|skip-)(.*)/) {
          $k = $1;
          $v = 0;
        }
        else {
          $v = 1;
        }
      }
      chomp($k); chomp($v);
      $cfg{$cur_sec}{$k} = $v;
    }
  }
  return %cfg;
}

# Dumb in-place merge of %$h2 into %$h1
# Returns $h1
sub _merge {
  my ($h1, $h2, $p) = @_;
  foreach my $k (keys %$h2) {
    # New section
    if(not $p and not exists $h1->{$k}) {
      $h1->{$k} = $h2->{$k};
    }
    # Existing section needs merge
    elsif(not $p and exists $h1->{$k}) {
      _merge($h1->{$k}, $h2->{$k}, $h1);
    }
    # Override existing keys
    elsif($p) {
      $h1->{$k} = $h2->{$k};
    }
  }
  $h1;
}

1;
