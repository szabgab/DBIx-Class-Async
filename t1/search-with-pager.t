

use strict;
use warnings;
use Test::More;
use File::Temp qw(tempfile);
use Future;
use lib 'lib', 't/lib';

use DBIx::Class::Async::Schema;

BEGIN {
    $SIG{__WARN__} = sub {};
}

# Helper to resolve Futures
sub wait_for {
    my $future = shift;
    return $future->get if ref($future) && $future->can('get');
    return $future;
}

# 1. Setup Physical Database
my ($fh, $filename) = tempfile(SUFFIX => '.db', UNLINK => 1);
my $dsn = "dbi:SQLite:dbname=$filename";

use TestSchema;
my $native_schema = TestSchema->connect($dsn);
$native_schema->deploy();

# 2. Seed Data (5 Orders)
# We need enough data to span multiple pages
my $u1 = $native_schema->resultset('User')->create({ name => 'Alice', age => 30 });
for my $i (1..5) {
    $u1->create_related('orders', {
        amount => $i * 10,
        status => ($i <= 3 ? 'pending' : 'shipped')
    });
}

# 3. Initialize Async Schema
my $async_schema = DBIx::Class::Async::Schema->connect(
    $dsn,
    {
        schema_class => 'TestSchema',
        workers      => 2 # Important for parallel execution!
    }
);

## Subtest: Basic Pagination Metadata
subtest 'search_with_pager metadata initialization' => sub {
    my $rs = $async_schema->resultset('Order');

    # We want pending orders, 2 per page
    my $paged_rs = $rs->search({ status => 'pending' }, { rows => 2, page => 1 });

    ok($paged_rs->is_paged, "ResultSet correctly identified as paged");
    is($paged_rs->{_attrs}{rows}, 2, "Rows attribute preserved");
    is($paged_rs->{_attrs}{page}, 1, "Page attribute preserved");

    my $pager = $paged_rs->pager;
    isa_ok($pager, 'DBIx::Class::Async::ResultSet::Pager');
    is($pager->entries_per_page, 2, "Pager inherited rows limit");
};

## Subtest: End-to-End Async Paging
subtest 'search_with_pager execution' => sub {
    my $rs = $async_schema->resultset('Order');

    # Fire the combined request
    # This fires TWO worker tasks: one for SELECT, one for SELECT COUNT
    my $future = $rs->search_with_pager(
        { status => 'pending' },
        { rows => 2, page => 1, order_by => 'amount' }
    );

    # wait_for should return the list provided by Future->done($rows, $pager)
    my ($rows, $pager) = wait_for($future);

    # 1. Check Data
    is(scalar @$rows, 2, "Found 2 rows for page 1");
    is($rows->[0]->amount, 10, "First row is correct (amount 10)");

    # 2. Check Pager (This verifies the parallel count query worked)
    # total_entries was resolved via $rs->count in the background
    my $total = wait_for($pager->total_entries);
    is($total, 3, "Total entries correctly reported as 3");
    is($pager->last_page, 2, "Correctly calculated that there are 2 pages total");
    ok($pager->has_next, "Pager knows there is a second page");
};

## Subtest: Paging with Relationships
subtest 'search_with_pager with related pivot' => sub {
    # Combine related_resultset + search_with_pager
    # Get orders for Alice (age 30), paged.
    my $future = $async_schema->resultset('User')
        ->search({ age => 30 })
        ->related_resultset('orders')
        ->search_with_pager({}, { rows => 1 });

    my ($rows, $pager) = wait_for($future);

    is(scalar @$rows, 1, "Pivoted and paged data successfully");
    is(wait_for($pager->total_entries), 5, "Total count correctly identified 5 orders for Alice");
};

done_testing();
