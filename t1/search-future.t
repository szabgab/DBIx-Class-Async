
use strict;
use warnings;
use Test::More;
use File::Temp qw(tempfile);
use lib 't/lib';
use TestSchema;
use DBIx::Class::Async::Schema;
use IO::Async::Loop;

BEGIN {
    $SIG{__WARN__} = sub {};
}

my $loop = IO::Async::Loop->new;
my ($fh, $db_file) = tempfile(SUFFIX => '.db', UNLINK => 1);

my $dsn    = "dbi:SQLite:dbname=$db_file";
my $schema = TestSchema->connect($dsn);
$schema->deploy;
$schema->resultset('User')->create({ name => 'Alice', email => 'a@test.com' });
$schema->resultset('User')->create({ name => 'Bob',   email => 'b@test.com' });

my $async_schema = DBIx::Class::Async::Schema->connect($dsn, {
    schema_class => 'TestSchema',
    async_loop   => $loop,
    workers      => 2,
});

subtest 'Verify search_future Alias' => sub {
    my $rs = $async_schema->resultset('User');

    my $future = $rs->search_future({ name => 'Bob' });
    isa_ok($future, 'Future', 'Method returns a Future object');

    $loop->await($future);

    my ($payload) = $future->get;

    my @results = (ref($payload) eq 'ARRAY') ? @$payload : ($payload);

    is(scalar @results, 1, 'Found exactly one record')
        or diag("Expected 1 result, got: " . scalar @results);

    if (@results) {
        my $row = $results[0];

        isa_ok($row, 'TestSchema::Result::User', 'Result is correctly inflated');
        is($row->name, 'Bob', 'Correct record retrieved');
    }

    done_testing;
};

done_testing;
