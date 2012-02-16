use Test::More tests => 3;
use ProcessLog;
use ZRMBackup;

my $pl = ProcessLog->null;
$pl->quiet(1);

my $bk1 = ZRMBackup->new($pl, "t/files/bk1");
my $bk2 = ZRMBackup->new($pl, "t/files/bk2");
my $bk3 = ZRMBackup->new($pl, "t/files/non_existant");
ok($bk1, 'can load 2.1 index');
ok($bk2, 'can load 2.0 index');
ok(!$bk3, 'fails on non-existant index');
