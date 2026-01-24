
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

# 3. The Test
subtest 'Validation of all_future() Bridge' => sub {
    my $rs = $async_schema->resultset('User')->search({ id => 1 });

    # Call all_future() - this triggers the DBIx::Class::Async::all bridge
    my $future = $rs->all_future;

    isa_ok($future, 'Future', 'all_future() returns a Future');

    my $results = $future->get;
    ok(ref($results) eq 'ARRAY', 'Returns an arrayref of results');

    # A. Verify Structure
    ok(ref($results) eq 'ARRAY', 'Returns an arrayref of results');
    is(scalar @$results, 1, 'Found exactly 1 row');

    # B. Verify Inflation & Hijacking
    my $row = $results->[0];
    ok($row, 'Result row exists');
    isa_ok($row, 'DBIx::Class::Async::Row', 'Row is hijacked by Async::Row');
    isa_ok($row, 'TestSchema::Result::User', 'Row is still a TestSchema User');

    # C. Verify Data Accuracy
    is($row->name, 'BottomUp User', 'Data was correctly retrieved from DB via worker');
    ok($row->in_storage, 'Row is correctly marked as in_storage');

    # D. Verify Class Name (Dynamic Hijacking check)
    like(ref($row), qr/Anon/, 'Row is blessed into a dynamic Anon class');
};

done_testing();
