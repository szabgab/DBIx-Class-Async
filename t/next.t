
use strict;
use warnings;
use Test::More;
use File::Temp qw(tempfile);
use IO::Async::Loop;
use lib 'lib';
use TestSchema;
use DBIx::Class::Async::Schema;

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

subtest 'Naked next() - Lazy Loading' => sub {
    my $rs = $async_schema->resultset('User')->search({ id => 1 });
    $rs->{_async_db}{_stats}{_queries} = 0;

    # We NEVER call $rs->all here. We go straight to next().
    my $row = $rs->next->get;

    ok($row, 'next() triggered a fetch on its own');
    is($row->id, 1, 'Got the correct row');
    is($rs->{_async_db}{_stats}{_queries}, 1, 'One query dispatched to worker');

    my $end = $rs->next->get;
    is($end, undef, 'End of results reached');
};

done_testing();
