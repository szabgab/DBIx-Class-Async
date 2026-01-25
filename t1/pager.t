
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

subtest 'Pager: Initialization and Math' => sub {
    # 1. Create a paged ResultSet (10 rows per page)
    my $rs = $async_schema->resultset('User')->page(1)->search(undef, { rows => 10 });
    my $pager = $rs->pager;

    isa_ok($pager, 'DBIx::Class::Async::ResultSet::Pager');
    is($pager->current_page, 1, 'Initial page is 1');

    # 2. Seed 25 users in the background
    $base_schema->resultset('User')->delete_all;
    for (1..25) { $base_schema->resultset('User')->create({ id => $_, name => "User $_" }) }

    # 3. Test total_entries (Future)
    my $f = $pager->total_entries;
    isa_ok($f, 'Future', 'total_entries returns a Future');

    my $total = $f->get;
    is($total, 25, 'Correctly counted 25 total entries');

    # 4. Test Math methods (now that total is cached)
    is($pager->last_page, 3, 'Last page is 3 (ceil(25/10))');
    is($pager->entries_on_this_page, 10, 'First page has 10 entries');
    is($pager->next_page, 2, 'Next page is 2');
    is($pager->previous_page, undef, 'No previous page for page 1');
};

subtest 'Pager: Boundary Conditions' => sub {
    # Test the very last page
    my $rs_last = $async_schema->resultset('User')->page(3)->search(undef, { rows => 10 });
    my $pager = $rs_last->pager;

    # Pre-inject total_entries to simulate a warm pager
    $pager->{_total_entries} = 25;

    is($pager->entries_on_this_page, 5, 'Last page (3) has exactly 5 entries');
    is($pager->next_page, undef, 'No next page after page 3');
    is($pager->previous_page, 2, 'Previous page is 2');
};

subtest 'Pager: ResultSet Chaining' => sub {
    my $rs = $async_schema->resultset('User')->page(1)->search(undef, { rows => 10 });
    my $pager = $rs->pager;

    # 1. Initialize the pager by fetching the total count
    $pager->total_entries->get; # Wait for the Future to resolve

    # 2. Now it is safe to chain
    my $next_rs = $pager->next_page_rs;
    isa_ok($next_rs, 'DBIx::Class::Async::ResultSet', 'next_page_rs returns a ResultSet');
    is($next_rs->{_attrs}->{page}, 2, 'New ResultSet is targeted at page 2');
};

done_testing();
