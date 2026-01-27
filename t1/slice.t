

use strict;
use warnings;
use Test::More;
use File::Temp qw(tempfile);
use lib 'lib', 't/lib';

use DBIx::Class::Async::Schema;

BEGIN {
    $SIG{__WARN__} = sub {};
}

# Helper to resolve your specific Future implementation
sub wait_for {
    my $future = shift;
    return $future->get if ref($future) && $future->can('get');
    return $future;
}

# 1. Create a physical temp file for the SQLite DB
my ($fh, $filename) = tempfile(SUFFIX => '.db', UNLINK => 1);
my $dsn = "dbi:SQLite:dbname=$filename";

# 2. Setup Database Schema (Native)
use TestSchema;
my $native_schema = TestSchema->connect($dsn);
$native_schema->deploy();

# 3. Seed Data
# User 1: Bob (40) -> 1 Order
my $u1 = $native_schema->resultset('User')->create({ name => 'Bob', age => 40 });
$u1->create_related('orders', { amount => 99.99, status => 'shipped' });

# User 2: Alice (30) -> 2 Orders
my $u2 = $native_schema->resultset('User')->create({ name => 'Alice', age => 30 });
$u2->create_related('orders', { amount => 10.00, status => 'pending' });
$u2->create_related('orders', { amount => 20.00, status => 'pending' });

# User 3: Charlie (25) -> 1 Order
my $u3 = $native_schema->resultset('User')->create({ name => 'Charlie', age => 25 });
$u3->create_related('orders', { amount => 5.00, status => 'cancelled' });

# 4. Initialize Async Schema
my $async_schema = DBIx::Class::Async::Schema->connect(
    $dsn,
    {
        schema_class => 'TestSchema',
        workers      => 2
    }
);

## Subtest 1: related_resultset (The Pivot)
subtest 'related_resultset() filtering' => sub {
    # Find orders belonging to people age 30 or older (Alice & Bob)
    my $orders_rs = $async_schema->resultset('User')
        ->search({ age => { '>=', 30 } })
        ->related_resultset('orders');

    # Inspect parent-side state
    is($orders_rs->{_attrs}{join}, 'user', "Parent correctly identified 'user' as reverse rel");

    # Execute async
    my $orders = wait_for($orders_rs->all);
    is(scalar @$orders, 3, "Found 3 orders total for Alice and Bob")
        or diag "Got " . scalar @$orders . " orders instead of 3";
};

## Subtest 2: slice (The Pagination)
subtest 'slice() pagination' => sub {
    # Get all orders sorted by amount ASC: [5.00, 10.00, 20.00, 99.99]
    my $rs = $async_schema->resultset('Order')->search({}, { order_by => 'amount' });

    # Slice indices 1 to 2 should be [10.00, 20.00]
    my $slice_rs = $rs->slice(1, 2);

    is($slice_rs->{_attrs}{offset}, 1, "Offset correctly set to 1");
    is($slice_rs->{_attrs}{rows}, 2, "Rows correctly set to 2");

    my $results = wait_for($slice_rs->all);
    is(scalar @$results, 2, "Retrieved exactly 2 rows from slice");
    is($results->[0]->amount, 10.00, "First sliced element is 10.00");
    is($results->[1]->amount, 20.00, "Second sliced element is 20.00");
};

## Subtest 3: Chaining Everything
subtest 'Chained related and slice' => sub {
    # Alice (age 30) has two orders (10.00 and 20.00).
    # Let's get her second most expensive order using slice.
    my $alice_second_order = $async_schema->resultset('User')
        ->search({ name => 'Alice' })
        ->related_resultset('orders')
        ->search({}, { order_by => { -desc => 'amount' } })
        ->slice(1, 1); # The second item

    my $results = wait_for($alice_second_order->all);
    is(scalar @$results, 1, "Found exactly one order in the chained slice");
    is($results->[0]->amount, 10.00, "Retrieved Alice's 2nd order (10.00)");
};

done_testing();
