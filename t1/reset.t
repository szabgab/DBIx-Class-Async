#!/usr/bin/env perl

use strict;
use warnings;
use Test::More;
use File::Temp qw(tempfile);
use lib 'lib', 't/lib';
use TestSchema;
use DBIx::Class::Async::Schema;

# Helper to resolve Futures
sub wait_for {
    my $future = shift;
    return $future->get if ref($future) && $future->can('get');
    return $future;
}

# 1. Setup Database & Schema
my ($fh, $filename) = tempfile(SUFFIX => '.db', UNLINK => 1);
my $dsn = "dbi:SQLite:dbname=$filename";

my $native_schema = TestSchema->connect($dsn);
$native_schema->deploy();

my $schema = DBIx::Class::Async::Schema->connect(
    $dsn, { schema_class => 'TestSchema', workers => 1 }
);

subtest 'ResultSet pointer reset' => sub {
    my $rs = $schema->resultset('User');

    # 1. Manually seed cache with 3 items
    $rs->set_cache([
        { id => 1, name => 'Alice' },
        { id => 2, name => 'Bob'   },
        { id => 3, name => 'Charlie' }
    ]);

    # 2. Inflate the rows by calling all()
    my $results = $schema->await($rs->all);
    is(scalar @$results, 3, "Inflated 3 rows");

    # 3. Simulate iterating through the results
    # (Assuming your next() method or manual logic increments _pos)
    $rs->{_pos} = 3;
    is($rs->{_pos}, 3, "Pointer moved to the end of the set");

    # 4. Call reset()
    my $returned_rs = $rs->reset;

    # 5. Verify
    is($rs->{_pos}, 0, "reset() moved the pointer back to 0");
    isa_ok($returned_rs, 'DBIx::Class::Async::ResultSet', "reset() returns self for chaining");

    # 6. Verify data accessibility after reset
    # If we had a next() method, it would now return Alice again.
    my $first_again = $rs->{_rows}->[$rs->{_pos}];
    is($first_again->name, 'Alice', "Data is still available and correct after reset");
};

done_testing;
