
use strict;
use warnings;
use Test::More;
use File::Temp qw(tempfile);
use IO::Async::Loop;
use lib 'lib';
use TestSchema;
use DBIx::Class::Async::Schema;

BEGIN {
    $SIG{__WARN__} = sub {};
}

# 1. Setup real temporary SQLite database
my ($fh, $filename) = tempfile(SUFFIX => '.db', UNLINK => 1);
my $dsn = "dbi:SQLite:dbname=$filename";

# Initialize and seed the DB so all_future has something to find
my $base_schema = TestSchema->connect($dsn);
$base_schema->deploy();
$base_schema->resultset('User')->create({
    id    => 1,
    name  => 'BottomUp User',
    email => 'bu@test.com'
});

# 2. Initialize the Async Engine
my $loop = IO::Async::Loop->new;
my $async_schema = DBIx::Class::Async::Schema->connect($dsn, {
    schema_class => 'TestSchema',
    async_loop   => $loop,
    workers      => 2,
});

subtest 'ResultSet Create - Basic' => sub {
    my $rs = $async_schema->resultset('User');

    # We do NOT provide an ID here
    my $future = $rs->create({
        name  => 'New Async User',
        email => 'async@example.com'
    });

    my $user = $future->get;

    isa_ok($user, 'DBIx::Class::Row', 'Returns a Row object');
    ok($user->id, 'User has an auto-incremented ID: ' . $user->id);
    is($user->name, 'New Async User', 'Name matches');
    ok($user->in_storage, 'Row is marked as in_storage');
};

done_testing();
