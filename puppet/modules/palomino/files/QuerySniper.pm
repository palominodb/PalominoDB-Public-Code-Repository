# QuerySniper.pm - a Perl module that kills running queries that meet given criteria.
# Copyright (C) 2013 PalominoDB, Inc.
# 
# You may contact the maintainers at eng@palominodb.com.
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

package QueryRules;
use strict;
use warnings FATAL => 'all';
use Data::Dumper;
use English qw(-no_match_vars);
use Text::ParseWords;
$Data::Dumper::Indent = 0;

use constant QSDEBUG => $ENV{QSDEBUG} || 0;

sub _d {
   my ($package, undef, $line) = caller 0;
   @_ = map { (my $temp = $_) =~ s/\n/\n# /g; $temp; }
        map { defined $_ ? $_ : 'undef' }
        @_;
   print STDERR "# $package:$line $PID ", join(' ', @_), "\n";
}

sub new {
   my ($class, $args) = @_;
   $args ||= {};

   $args->{list} = ();
   $args->{syms} = {};
   $args->{rules} = undef;
   $args->{config} = {};

   $args->{reserved_syms} = ['User', 'Host', 'Db', 'Time', 'Command', 'State', 'Info'];
   $args->{special_syms} = ['log_file', 'log_level', 'querylog', 'querylog_dsn', 'pretend', 'usestatus', 'usevars'];

   bless $args, $class;
   $args->set_sym('User', q#$p->{User}#, 'strref');
   $args->set_sym('Host', q#$p->{Host}#, 'strref');
   $args->set_sym('Db', q#$p->{Db}#, 'strref');
   $args->set_sym('Time', q#$p->{Time}#, 'intref');
   $args->set_sym('Command', q#$p->{Command}#, 'strref');
   $args->set_sym('State', q#$p->{State}#, 'strref');
   $args->set_sym('Info', q#$p->{Info}#, 'strref');
   return $args;
}

sub compile {
   my ($self) = @_;
   my @lines = ();
   open my $lfh, '>>', $self->config('log_file');
   push @lines, "sub {";
   push @lines, 'my ($p) = @_;';
   push @lines, 'my $pass = 1;';
   push @lines, 'my $logfh = $lfh;';
   push @lines, 'my $log_level = '. $self->config('log_level') .';';
   push @lines, $self->to_perl();
   push @lines, 'print $logfh "killed: ". Dumper($p) ."\n" if(!$pass and $log_level >= 2);';
   push @lines, 'return $pass;';
   push @lines, "}";

   my $code = join("\n", @lines);
   print $lfh "Compiled sub: @lines\n" if($self->config('log_level') >= 3);
   QSDEBUG && _d("sniper sub: @lines");
   $self->{rules} = eval "$code" or die("Error in sniper routine: $EVAL_ERROR");
   return 1;
}

sub run {
   my ($self, $p) = @_;
   map {
     if(defined($p->{$_})) {
       $p->{$_} = lc($p->{$_});
     }
     else {
       my ($type) = @{$self->_vtype($_)};
       if($type eq 'sym' and $self->{syms}->{$_}->[0] eq 'intref') {
         # Special meaning, not reaally a string.
         # This is the special value NaN not "NaN".
         # See perlop for details.
         $p->{$_} = 'NaN';
       }
       else {
         $p->{$_} = 'null';
       }
     }
   } keys %$p;
   my $r = undef;
   QSDEBUG && _d('evaluating proc:', Dumper($p));
   eval {
     $r = $self->{rules}->($p);
   };
   if($@) {
     QSDEBUG && _d('proc:', Dumper($p), 'died with:', $@);
     die();
   }
   return $r;
}

sub config {
   my ($self, $var, $val) = @_;
   my $old = $self->{config}->{$var};
   $self->{config}->{$var} = $val if(defined($val));
   return $old;
}

sub load {
   my ($self,$file) = @_;
   open my $cfh, "<", $file or die($OS_ERROR);

   $self = __PACKAGE__->new if($self->config('log_file'));

   while (<$cfh>) {
      if($self->_parse($_)) {
         if($self->config('log_fh') and $self->config('log_level')) {
            my $fh = $self->config('log_fh');
            print($fh "parsed: $_") if($self->config('log_level') >= 3);
         }
      }
      else {
         die("Unable to compile rules. Encountered unknown statement: '$_'");
      }
   }

   die("log_file is required") unless($self->config('log_file'));
   die("log_level is required") unless($self->config('log_level'));

   return 1;
}

sub _parse {
   my ($self, $line) = @_;
   my %as = ();
   my @toks = shellwords($line);

   if(not defined($toks[0]) or $toks[0] =~ /^\s*#/) {
      return 1;
   }
   $as{original_line} = $line;
   $as{original_toks} = [@toks];
   $self->_action(\%as,\@toks);
   QSDEBUG && _d(Dumper(\@toks));
   $self->_expressions(\%as, \@toks);
   $Data::Dumper::Indent = 3;
   QSDEBUG && _d(Dumper(\%as));
   $Data::Dumper::Indent = 0;

   push @{$self->{list}}, \%as;
   return 1;
}

sub recompile {
   my ($self) = @_;
   my @origlist = @{$self->{list}};
   $self->{list} = ();
   foreach my $l (@origlist) {
      my %as = ();
      $as{original_line} = $l->{original_line};
      $as{original_toks} = $l->{original_toks};
      my @toks = @{$l->{original_toks}};
      $self->_action(\%as,\@toks);
      $self->_expressions(\%as, \@toks);
      push @{$self->{list}}, \%as;
   }
   $self->compile();
   return 1;
}

sub set_sym {
   my ($self, $name, $value, $type) = @_;
   QSDEBUG && _d('n:', $name, 'v:', $value, 't:', $type);
   $self->{syms}->{$name} = [$type, $value];
}

sub del_sym {
   my ($self, $name) = @_;
   delete $self->{syms}->{$name};
}

sub _expr_to_perl {
   my ($self,$expr, $rest) = @_;
   $rest ||= "";
   $expr = $expr->[0];
   my $op = $expr->{op};
   if($op eq 'and' or $op eq 'or') {
      my $s1 = $self->_expr_to_perl($expr->{opr1});
      my $s2 = $self->_expr_to_perl($expr->{opr2});
      return "$s1 $op $s2";
   }

   my @p1 = @{$expr->{opr1}};
   my @p2 = @{$expr->{opr2}};

   my $p1s = "";
   my $p2s = "";

   QSDEBUG && _d("compiling:", %$expr);
   QSDEBUG && _d("op:", $op);
   ($p1s, $op) = $self->_val_to_perl($op, @p1);
   ($p2s, $op) = $self->_val_to_perl($op, @p2);

   return "$p1s $op $p2s" if($op);
   QSDEBUG && _d(@$p1s, @$p2s);
   return "@$p1s @$p2s";
}

sub _val_to_perl {
   my ($self, $op, @val) = @_;
   my $p2s = undef;
   QSDEBUG && _d($op, @val);
   if($val[0] eq 'str') {
      if($op) {
         die("non-sensical operator '$op' for string operands") if($op eq '>=' or $op eq '<=' or $op eq '>' or $op eq '<');
         $op = 'eq' if($op eq '==');
         $op = 'ne' if($op eq '!=');
      }
      $p2s = lc(qq("$val[1]"));
   }
   elsif($val[0] eq 'sym') {
      my $v = $self->{syms}->{$val[1]};
      if($v->[0] eq 'strref') {
         if($op) {
            die("non-sensical operator '$op' for string operands") if($op eq '>=' or $op eq '<=' or $op eq '>' or $op eq '<');
            $op = 'eq' if($op eq '==');
            $op = 'ne' if($op eq '!=');
         }
         $p2s = $v->[1];
      }
      elsif($v->[0] eq 'intref') {
         $p2s = $v->[1];
      }
      elsif($v->[0] eq 'int') {
         $p2s = $v->[1];
      }
      elsif($v->[0] eq 'str') {
         if($op) {
            die("non-sensical operator '$op' for string operands") if($op eq '>=' or $op eq '<=' or $op eq '>' or $op eq '<');
            $op = 'eq' if($op eq '==');
            $op = 'ne' if($op eq '!=');
         }
         $p2s = lc(qq("$v->[1]"));
      }
   }
   elsif($val[0] eq 'int') {
      $p2s = $val[1];
   }
   elsif($val[0] eq 'regex') {
      die("operator not =~ when operand regex") if($op and $op ne '=~');
      $p2s = $val[1];
   }
   QSDEBUG && _d($p2s, $op);
   return ($p2s, $op);
}

sub to_perl {
   my ($self) = @_;
   my @lines;
   foreach my $r (@{$self->{list}}) {
      next if($r->{action} eq 'set');
      my $cond = $self->_expr_to_perl($r->{expressions});
      push @lines, "if( $cond ) {";
      push @lines, '   $pass=0;' if($r->{action} eq 'kill');
      push @lines, '   $pass=1;' if($r->{action} eq 'pass');
      push @lines, '   _logquery($p);' if($r->{logquery} eq 'pass');
      push @lines, '  return $pass;' if($r->{immediate});
      push @lines, '}';
   }
   return @lines;
}

sub _logquery {
  my ($self, $p) = @_;
  # TODO
}

# Return a nicely formatted stacktrace.
sub _stack {
  my ($level) = @_;
  $level ||= 10;
  my $out = "";
  my $i=0;
  my ($package, $file, $line, $sub) = caller($i+1); # +1 hides _stack from the stack trace.
  $i++;
  if($package) {
    $out .= "Stack trace:\n";
  }
  else {
    $out .= "No stack data available.\n";
  }
  while($package and $i < $level) {
    $out .= " "x$i . "$package  $file:$line  $sub\n";
    ($package, $file, $line, $sub) = caller($i+1);
    $i++;
  }
  chomp($out);
  $out;
}

sub _expect {
   my ($t, $r) = @_;
   my $ret = eval {
      $t =~ $r; $1;
   };
   if($EVAL_ERROR) {
      print STDERR "trying to match: $r\n";
      print STDERR "$@";
      print STDERR _stack(), "\n";
   }
   return $ret;
}

sub _action {
   my ($self, $st, $toks) = @_;
   my $t = shift @$toks;
   $st->{immediate} = 0;
   $st->{logquery} = 0;
   my $action = _expect($t, qr/(pass|kill|set)/) or die("Unknown action: $t from '$t @$toks' on line '$st->{original_line}'");
   $st->{action} = $action;
   if($action eq "pass" or $action eq "kill") {
      if(_expect($toks->[0], qr/(now)/)) {
         $st->{immediate} = 1;
         shift @$toks;
      }
      if(_expect($toks->[0], qr/(log)/)) {
         $st->{logquery} = 1;
         shift @$toks;
      }
   }
   elsif($action eq "set") {
      # XXX: No modifiers for set as of yet.
   }
   return 1;
}

sub _expressions {
   my ($self,$st, $toks) = @_;
   if($st->{action} eq "pass" or $st->{action} eq "kill") {
      $st->{expressions} ||= ();
      while(my $e = $self->_expression($st, $toks)) {
         push @{$st->{expressions}}, $e;
      }
   }
   elsif($st->{action} eq "set") {
      _expect($toks->[0], qr/^([A-Za-z_]+)$/) or die("Invalid symbol name: $toks->[0] on line '$st->{original_line}'");
      my $sym = shift @$toks;

      $_ eq $sym && die("Cannot set reserved symbol: $sym at $st->{original_line}") for @{$self->{reserved_syms}};

      _expect($toks->[0], qr/^(=)$/) or die("Missing '=' (got '$toks->[0]') on line '$st->{original_line}'");
      shift @$toks;

      my $val = $self->_value($st, $toks);
      shift @$toks;

      if(scalar grep { $_ eq $sym } @{$self->{special_syms}}) {
         $self->config($sym, $val->[1]);
      }
      else {
         $self->set_sym($sym, reverse(@$val))
      }
   }
   return 1;
}

sub _term {
   my ($self, $st, $toks) = @_;

   #QSDEBUG && _d("_term: st:", Dumper($st), "   toks:", Dumper($toks));

   my $param1 = $self->_value($st, $toks);
   $param1 or die("Unknown parameter starting at '@$toks' on line '$st->{original_line}'");
   shift @$toks;

   my $oper = _expect($toks->[0], qr/(>=|<=|!=|=~|==|>|<)/)
      or die("Unknown operator '$toks->[0]' starting at '@$toks' on line '$st->{original_line}'");
   shift @$toks;

   my $param2 = $self->_value($st, $toks);
   $param2 or die("Unknown parameter starting at '@$toks' on line '$st->{original_line}'");
   shift @$toks;
   return {op => $oper, opr1 => $param1, opr2 => $param2}
}

sub _expression {
   my ($self, $st, $toks, $e) = @_;

   return $e if(scalar(@$toks) == 0);

   QSDEBUG && _d('expr1: ', Dumper($e), 'toks: ', scalar @$toks);

   if($e) {
      $e->{opr2} = [$self->_term($st, $toks)];
   }
   else {
      $e = $self->_term($st, $toks);
   }

   QSDEBUG && _d('expr2: ', Dumper($e), 'toks: ', scalar @$toks);

   return $e if(scalar(@$toks) == 0);

   if($toks->[0] and _expect($toks->[0], qr/^(and|or)$/)) {
      QSDEBUG && _d("FOUND AND/OR EXPRESSION");
      $e = {op => $toks->[0], opr1 => [$e], opr2 => undef};
      shift @$toks;
      $e = $self->_expression($st, $toks, $e);
   }

   QSDEBUG && _d('expr3: ', Dumper($e), 'toks: ', scalar @$toks);
   return $e;

}

sub _vtype {
   my ($self, $v) = @_;

   if(not defined($v)) {
      return ['undef', 'null'];
   }
   elsif(exists $self->{syms}->{$v}) {
      return ['sym', $v];
   }
   elsif($v =~ /(\d+)/) {
      return ['int', int($1)];
   }
   elsif($v =~ /^\/.*\/i?$/) {
      return ['regex', $v];
   }
   elsif($v =~ /(.*)/) { # Strings have lowest priority
      return ['str', $1];
   }
   return undef;
}

sub _value {
   my ($self, $st, $toks) = @_;

   return $self->_vtype($toks->[0]);
}

1;

package QuerySniper;

use strict;
use warnings FATAL => 'all';
use Data::Dumper;
use English qw(-no_match_vars);
$Data::Dumper::Indent = 0;

use constant QSDEBUG => $ENV{QSDEBUG} || 0;

sub _d {
   my ($package, undef, $line) = caller 0;
   @_ = map { (my $temp = $_) =~ s/\n/\n# /g; $temp; }
        map { defined $_ ? $_ : 'undef' }
        @_;
   print STDERR "# $package:$line $PID ", join(' ', @_), "\n";
}

sub new {
   my ($class, $args) = @_;
   die("Query sniper requires path to configuration file as its sole argument") unless($args->{o});
   die("Unable to open configuration") unless(-r $args->{o});

   my $qr = QueryRules->new();
   $qr->load($args->{o});
   $qr->compile();
   $args->{qr} = $qr;

   bless $args, $class;
   return $args;
}

sub watch_event {
   my ($self, $watches) = @_;
   my $qslog_fh = $self->{config}->{log_fh};
   my $data = undef;
   foreach my $w (@$watches) {
      my ($wn, $opts) = split /:/, $w->{name}, 2;
      if($wn eq 'Processlist') {
         my @t = $w->{module}->get_last_data();
         $data = $t[0];
      }
   }
   $data = $self->{dbh}->selectall_arrayref("SHOW FULL PROCESSLIST", { Slice => {} }) unless($data);
   if($self->{qr}->config('usestatus')) {
      foreach my $s (@{$self->{dbh}->selectall_arrayref("SHOW GLOBAL STATUS", { Slice => {} })}) {
         $self->{qr}->del_sym($s->{'Variable_name'});
         my ($type, $val) = @{$self->{qr}->_vtype($s->{'Value'})};
         $self->{qr}->set_sym($s->{'Variable_name'}, $val, $type );
      }
   }
   if($self->{qr}->config('usevars')) {
      foreach my $s (@{$self->{dbh}->selectall_arrayref("SHOW GLOBAL VARIABLES", { Slice => {} })}) {
         $self->{qr}->del_sym($s->{'Variable_name'});
         my ($type, $val) = @{$self->{qr}->_vtype($s->{'Value'})};
         $self->{qr}->set_sym($s->{'Variable_name'}, $val, $type );
      }
   }
   $self->{qr}->recompile() if($self->{qr}->config('usevars') or $self->{qr}->config('usestatus'));
   foreach my $p (@$data) {
      my $r = $self->{qr}->run($p);
      QSDEBUG && _d("Proc: ", Dumper($p), "  Result: ", $r);
      unless($r) {
         QSDEBUG && _d("KILL $p->{Id}");
         $self->{dbh}->do("KILL $p->{Id}") unless ($self->{qr}->config('pretend'));
      }
   }
   return 1;
}

sub done {
   my ($self) = @_;
}

sub set_dbh {
   my ($self, $dbh) = @_;
}

1;
