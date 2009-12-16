package TableRotator;
use DBI;
use ProcessLog;
use DateTime;

sub new {
  my $class = shift;
  my ($dbh, $plog, $host, $user, $pass, $format) = @_;
  $format ||= "%Y%m%d"
  my $self = {};
  $self->{dbh} = $dbh;
  $self->{plog} = $plog;
  $self->{host} = $host;
  $self->{user} = $user;
  $self->{pass} = $pass;
  $self->{format} = $format;

  bless $self, $class;
  return $self;
}

sub date_rotate {
  my ($self, $schema, $table) = @_;

  my $dt = DateTime->new;
  my $rot_table = $dt->strftime("${table}_$format")
  $self->{plog}->d("Going to rotate `$schema`.`$table` to `$schema`.`$rot_table`");
  my $tmp_table = substr("${table}_". $self->{plog}->runid(), 0, 64);

  eval {
    local $SIG{INT} = sub { $self->{plog}->i("caught and ignored SIGINT during table rotate."); };
    local $SIG{TERM} = sub { $self->{plog}->i("caught and ignored SIGTERM during table rotate."); };

    $self->{plog}->d("Creating new table:", "`$schema`.`$tmp_table`");
    $self->{dbh}->do("CREATE TABLE `$schema`.`$tmp_table` LIKE `$schema`.`$table`")
      or $self->{plog}->e("Unable to create new table.") and die("Unable to create new table");

    $self->{plog}->d("Atomically renaming tables:\n",
      "  `$schema`.`$table` to `$schema`.`$rot_table`\n",
      "  `$schema`.`$tmp_table` to `$schema`.`$table`"
    );
    $self->{dbh}->do("RENAME TABLE `$schema`.`$table` TO `$schema`.`$rot_table`, `$schema`.`$tmp_table` TO `$schema`.`$table`")
      or $self->{plog}->e("Failed to rename tables.") and die("Failed to rename tables");
  }
  if($@) {
    chomp($@);
    $self->{plog}->es("Failure to rotate tables:", $@);
    die("Table rotate failure");
  }
  $self->{plog}->d("rotated `$schema`.`$table` to `$schema`.`$rot_table`");
  return 1;
}

1;
