
use strict;
use warnings;
use Test::More;
use File::Temp qw(tempfile);
use lib 'lib', 't/lib';

BEGIN {
    $SIG{__WARN__} = sub {};
}

# Helper to resolve Futures
sub wait_for {
    my $future = shift;
    return $future->get if ref($future) && $future->can('get');
    return $future;
}

# 1. Setup Database
my ($fh, $filename) = tempfile(SUFFIX => '.db', UNLINK => 1);
my $dsn = "dbi:SQLite:dbname=$filename";

use TestSchema;
my $native_schema = TestSchema->connect($dsn);
$native_schema->deploy();

# 2. Initialize Async Schema
use DBIx::Class::Async::Schema;
my $async_schema = DBIx::Class::Async::Schema->connect(
    $dsn, { schema_class => 'TestSchema', workers => 1 }
);


## Subtest 1: Manual Raw Data Inflation
subtest 'set_cache with raw data' => sub {
    my $rs = $async_schema->resultset('User');

    # Simulate data that might have come from a manual JSON import or Prefetch
    my $raw_data = [
        { id => 999, name => 'Cache Test User', age => 50 }
    ];

    $rs->set_cache($raw_data);

    ok($rs->{_is_prefetched}, "ResultSet marked as prefetched");
    is($rs->{_entries}, $raw_data, "Raw data stored in _entries");

    # Trigger all() - this should use lines 129-138 of your all() method
    my $results = wait_for($rs->all);

    is(scalar @$results, 1, "Returned 1 row");
    isa_ok($results->[0], 'TestSchema::Result::User', "Raw hash was inflated into a Result object");
    is($results->[0]->name, 'Cache Test User', "Attribute 'name' is correct");

    # Verify that it didn't hit the worker (no 'all' STAGE logs should appear in console)
};

## Subtest 2: Direct Object Caching
subtest 'set_cache with objects' => sub {
    my $rs = $async_schema->resultset('User');

    # Create a real object via the native schema for the sake of the test
    my $user_obj = $native_schema->resultset('User')->new_result({
        id => 888,
        name => 'Existing Object'
    });

    $rs->set_cache([ $user_obj ]);

    my $results = wait_for($rs->all);

    is($results->[0], $user_obj, "all() returned the exact same object instance");
    is($results->[0]->id, 888, "Object state preserved");
};

## Subtest 3: Position Reset
subtest 'set_cache resets position' => sub {
    my $rs = $async_schema->resultset('User');

    # Set initial cache
    $rs->set_cache([ { name => 'User A' } ]);
    wait_for($rs->all);

    # Manually simulate being at the end of the list
    $rs->{_pos} = 1;

    # Update cache
    $rs->set_cache([ { name => 'User B' } ]);

    is($rs->{_pos}, 0, "set_cache reset the resultset position to 0");

    my $results = wait_for($rs->all);
    is($results->[0]->name, 'User B', "New cache successfully replaced old data");
};

done_testing();
