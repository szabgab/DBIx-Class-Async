
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

subtest 'Smart all() - Caching and Iterator Validation' => sub {
    my $rs = $async_schema->resultset('User')->search({ id => 1 });

    # Reset stats for a clean run
    $rs->{_async_db}{_stats}{_queries} = 0;

    # --- FIRST CALL ---
    my $future1 = $rs->all;
    my $results1 = $future1->get; # Blocks until worker returns

    is(scalar @$results1, 1, 'First call: Found 1 user');
    is($rs->{_async_db}{_stats}{_queries}, 1, 'Stats: Query count is 1 after first fetch');
    isa_ok($results1->[0], 'DBIx::Class::Async::Row', 'Data is correctly inflated/hijacked');

    # --- SECOND CALL (Cache Hit) ---
    my $results2 = $rs->all->get;

    is(scalar @$results2, 1, 'Second call: Still has 1 user');
    is($rs->{_async_db}{_stats}{_queries}, 1, 'Stats: Query count remains 1 (Cache HIT)');
    is($results1->[0], $results2->[0], 'Both calls returned the exact same object instances');

    # --- ITERATOR INTEGRATION ---
    # Since all() was called, the buffer is full and pos is 0.
    my $next_row = $rs->next->get;
    ok($next_row, 'next() successfully retrieved row from all() buffer');
    is($next_row->id, 1, 'Iterator data matches cached data');

    # Check that we reached the end
    my $end = $rs->next->get;
    is($end, undef, 'Iterator correctly reached the end of the buffer');
};

done_testing();
