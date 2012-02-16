use Test::More tests => 1;
use ProcessLog;
use ZRMBackup;

my $pl = ProcessLog->null;
$pl->quiet(1);

my $bk4 = ZRMBackup->new($pl, "t/files/bk1");
ok($bk4, "was able to ignore invalid lines");
