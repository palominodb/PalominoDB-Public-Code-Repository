use Test::More tests => 4;
use ProcessLog;
use ZRMBackup;

my $pl = ProcessLog->null;
$pl->quiet(1);

my $bk1 = ZRMBackup->new($pl, "t/files/bk1");
ok($bk1, "parsed ok");
ok(!$bk1->open_last_backup, "fail to open non-existant backup");

my $bk3 = ZRMBackup->new($pl, "t/files/bk3");
ok($bk3, "parsed ok");
ok($bk3->open_last_backup, "opened bk2");
