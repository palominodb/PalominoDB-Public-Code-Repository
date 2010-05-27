use Test::More tests => 1;
use ProcessLog;
use ZRMBackup;

my $pl = ProcessLog->null;
$pl->quiet(1);

my $bk4 = ZRMBackup->new($pl, "t/files/bk5");
ok($bk4, "gigabyte backup-size doesn't throw warning");

