#!/usr/bin/env perl

use strict;
use warnings;
use utf8;

use File::Temp;
use Test::More;
use Test::Exception;

use FindBin qw($Bin);
use lib "$Bin/../lib";
use lib "$Bin/lib";

use DBI;
use TestSchema;
use IO::Async::Loop;
use DBIx::Class::Async;
use DBIx::Class::Async::Schema;

my ($fh, $db_file) = File::Temp::tempfile(SUFFIX => '.db', UNLINK => 1);

# 1. Initialise the Async Schema
my $schema = DBIx::Class::Async::Schema->connect(
    "dbi:SQLite:dbname=$db_file",
    undef, undef, {},
    { workers => 2, schema_class => 'TestSchema' }
);

# 2. Get the loop from the internal async_db object
my $loop = $schema->{_async_db}->{_loop};

# 3. Deploy automatically!
$loop->await($schema->deploy);

# Test 1: Basic schema connection
subtest 'Basic schema connection' => sub {

    my $loop = IO::Async::Loop->new;
    my $schema;
    lives_ok {
        $schema = DBIx::Class::Async::Schema->connect(
            "dbi:SQLite:dbname=$db_file",
            undef,
            undef,
            {},
            {
                workers      => 2,
                schema_class => 'TestSchema',
                loop         => $loop
            }
        );
    } 'Schema connects successfully';

    isa_ok($schema, 'DBIx::Class::Async::Schema');

    my @sources = $schema->sources;
    is(scalar @sources, 3, 'Has 3 sources');

    ok(grep(/^User$/, @sources), 'Has User source');
    ok(grep(/^Order$/, @sources), 'Has Order source');

    lives_ok { $schema->disconnect } 'Can disconnect';
};

# Test 2: Simple user CRUD
subtest 'Simple user CRUD' => sub {

    my $loop   = IO::Async::Loop->new;
    my $schema = DBIx::Class::Async::Schema->connect(
        "dbi:SQLite:dbname=$db_file",
        undef,
        undef,
        {},
        {
            workers      => 2,
            schema_class => 'TestSchema',
            loop         => $loop
        }
    );

    my $user_rs = $schema->resultset('User');
    isa_ok($user_rs, 'DBIx::Class::Async::ResultSet');

    # Create
    my $user = $user_rs->create({
        name   => 'Test User',
        email  => 'test@example.com',
        active => 1,
    })->get;

    isa_ok($user, 'DBIx::Class::Async::Row');
    is($user->name, 'Test User');
    is($user->email, 'test@example.com');

    # Find
    my $found = $user_rs->find($user->id)->get;
    isa_ok($found, 'DBIx::Class::Async::Row');
    is($found->name, 'Test User');

    # Update
    my $updated = $found->update({
        name   => 'Updated User',
        active => 0,
    })->get;
    is($updated->name, 'Updated User');
    is($updated->active, 0);

    $schema->disconnect;
};

# Test 3: Order operations
subtest 'Order operations' => sub {

    my $loop   = IO::Async::Loop->new;
    my $schema = DBIx::Class::Async::Schema->connect(
        "dbi:SQLite:dbname=$db_file",
        undef,
        undef,
        {},
        {
            workers      => 2,
            schema_class => 'TestSchema',
            loop         => $loop
        }
    );

    # Create user
    my $user = $schema->resultset('User')->create({
        name  => 'Order User',
        email => 'order@example.com',
    })->get;

    # Create order
    my $order = $schema->resultset('Order')->create({
        user_id => $user->id,
        amount  => 49.99,
        status  => 'paid',
    })->get;

    isa_ok($order, 'DBIx::Class::Async::Row');
    is($order->amount, 49.99);
    is($order->user_id, $user->id);

    # Test belongs_to
    my $order_user = $order->user->get;
    is($order_user->id, $user->id, 'belongs_to works');

    # Test has_many
    my $user_orders = $user->orders->all->get;
    isa_ok($user_orders, 'ARRAY');
    is(scalar @$user_orders, 1, 'User has 1 order');
    is($user_orders->[0]->id, $order->id, 'Correct order');

    $schema->disconnect;
};

# Test 4: Search and count
subtest 'Search and count' => sub {

    my $loop   = IO::Async::Loop->new;
    my $schema = DBIx::Class::Async::Schema->connect(
        "dbi:SQLite:dbname=$db_file",
        undef,
        undef,
        {},
        {
            workers      => 2,
            schema_class => 'TestSchema',
            loop         => $loop
        }
    );

    my $user_rs = $schema->resultset('User');

    my $count_before = $user_rs->count->get;  # count() returns Future, so we need ->get

    # Create test data
    $user_rs->create({ name => 'Search Test 1', email => 's1@test.com', active => 1 })->get;
    $user_rs->create({ name => 'Search Test 2', email => 's2@test.com', active => 0 })->get;

    my $count_after = $user_rs->count->get;
    is($count_after, $count_before + 2, 'Count increased');

    # Search active
    my $active_search_rs    = $user_rs->search({ active => 1 });
    my $active_users_future = $active_search_rs->all_future;
    my $active_users        = $active_users_future->get;
    isa_ok($active_users, 'ARRAY', 'search returns array of rows');

    # Count active
    my $active_count_rs     = $user_rs->search({ active => 1 });
    my $active_count_future = $active_count_rs->count_future;
    my $active_count        = $active_count_future->get;
    cmp_ok($active_count, '>=', 1, 'Has active users');

    $schema->disconnect;
};

END {
    unlink $db_file if -e $db_file;
}

done_testing();
