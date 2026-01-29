#!/usr/bin/env perl

use strict;
use warnings;
use Test::More;
use File::Temp qw(tempfile);
use IO::Async::Loop;
use Future;
use Scalar::Util qw(blessed);

use lib 'lib', 't/lib';
use DBIx::Class::Async::Schema;

my $loop = IO::Async::Loop->new;
my ($fh, $db_file) = tempfile(SUFFIX => '.db', UNLINK => 1);
my $dsn = "dbi:SQLite:dbname=$db_file";

my $schema = DBIx::Class::Async::Schema->connect(
    $dsn, undef, undef, {},
    {
        schema_class => 'TestSchema',
        loop         => $loop,
        workers      => 2,
    }
);

# 3. Deploy and Seed
$schema->await($schema->deploy);

# Create a User with specific settings (testing the JSON inflator)
# and multiple orders (testing prefetch)
my $user = $schema->resultset('User')->create({
    name     => 'Async Developer',
    email    => 'dev@example.com',
    age      => 30,
    active   => 1,
    settings => { theme => 'dark', notifications => 1 },
    balance  => 1000.50,
})->get;

$schema->await($user->create_related('orders', { amount => 150.00, status => 'completed' }));
$schema->await($user->create_related('orders', { amount => 42.99,  status => 'pending'   }));


subtest 'Search with Prefetch and HashRefInflator' => sub {
    # We want a User, their Orders, and we want it as a plain Hash
    my $future = $schema->search_with_prefetch(
        'User',
        { email => 'dev@example.com' },
        ['orders'],
        { result_class => 'DBIx::Class::ResultClass::HashRefInflator' }
    );

    # We use list context to "unwrap" the results from the future
    my $results = $schema->await($future);

    # Now $results should be the ARRAY ref you expect
    is(ref($results), 'ARRAY', 'Results is an array reference');
    is(scalar @$results, 1, 'Found exactly 1 user');

    my $user = $results->[0];
    is($user->{name}, 'Async Developer', 'Correct user name');

    # Verify prefetch worked
    is(ref($user->{orders}), 'ARRAY', 'Orders prefetched as array');
    is(scalar @{$user->{orders}}, 2, 'Found both orders');

    is($user->{orders}[0]{amount}, 150, 'First order amount matches');

    # Validate JSON Inflator
    is(ref $user->{settings}, 'HASH', 'Settings column was correctly inflated from JSON');
    is($user->{settings}{theme}, 'dark', 'Inflated data is correct');
    is(ref $user->{settings}, 'HASH', 'Settings column inflated in HashRef');
};

subtest 'Search with Prefetch (Object Mode)' => sub {
    # Testing without HashRefInflator to ensure Row objects work
    my $future = $schema->search_with_prefetch(
        'User',
        { email => 'dev@example.com' },
        ['orders']
    );

    my $results = $schema->await($future);
    my $u = $results->[0];

    ok(blessed($u), 'Result is a blessed Row object');
    ok($u->can('orders'), 'Object has orders relationship method');

    my $orders_future = $u->orders->all;
    my $orders = $schema->await($orders_future);
    is(scalar @$orders, 2, 'Related objects are accessible');
};

done_testing();
