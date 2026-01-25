
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

my $base_schema = TestSchema->connect($dsn);
$base_schema->deploy();

# Create user
my $user = $base_schema->resultset('User')->create({
    id     => 1,
    name   => 'BottomUp User',
    email  => 'bu@test.com',
    active => 1,
});

# Create some orders for the user
$base_schema->resultset('Order')->create({
    user_id => 1,
    status  => 'pending',
    amount  => 100.00,
});

$base_schema->resultset('Order')->create({
    user_id => 1,
    status  => 'completed',
    amount  => 50.00,
});

$base_schema->storage->disconnect;


# 2. Initialize the Async Engine
my $loop = IO::Async::Loop->new;
my $async_schema = DBIx::Class::Async::Schema->connect($dsn, {
    schema_class => 'TestSchema',
    async_loop   => $loop,
    workers      => 2,
});

subtest 'Relationship Pivoting (search_related)' => sub {
    my $user_future = $async_schema->resultset('User')->find(1);
    my $user = $user_future->get;

    my $orders_rs = eval { $user->search_related_rs('orders') };

    if ($@) {
        fail("search_related_rs failed: $@");
        return;
    }

    isa_ok($orders_rs, 'DBIx::Class::Async::ResultSet', 'Pivoted to Orders');
    is($orders_rs->{_source_name}, 'TestSchema::Result::Order', 'Source name updated to Order result class');

    my $orders = $orders_rs->all_future->get;

    is(scalar(@$orders), 2, 'User has 2 orders');

    if (@$orders) {
        is($orders->[0]->user_id, $user->id, 'Order belongs to the correct user');
    }
};

subtest 'Chained search_related' => sub {
    my $user = $async_schema->resultset('User')->find(1)->get;

    my $recent_orders = $user->search_related('orders',
        { status   => 'pending' },
        { order_by => { -desc => 'id' },
          rows     => 1 }
    )->all_future->get;

    ok(scalar @$recent_orders <= 1, "Chained search attributes respected (rows => 1)");
    is(scalar @$recent_orders, 1, "Found 1 pending order");
    is($recent_orders->[0]->status, 'pending', "Order status is pending");
};

done_testing();
