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

# 1. Setup Database
my ($fh, $filename) = tempfile(SUFFIX => '.db', UNLINK => 1);
my $dsn = "dbi:SQLite:dbname=$filename";
TestSchema->connect($dsn)->deploy();

# 2. Setup Async
my $loop = IO::Async::Loop->new;
my $async_schema = DBIx::Class::Async::Schema->connect($dsn, {
    schema_class => 'TestSchema',
    async_loop   => $loop,
});

sub wait_for {
    my $f = shift;
    my $res;
    $f->on_ready(sub { $res = shift->result; $loop->stop; });
    $loop->run;
    return $res;
}

# Seed Data
my $schema = TestSchema->connect($dsn);
my $dave = $schema->resultset('User')->create({ name => 'Dave', age => 50 });
$dave->create_related('orders', { amount => 10, status => 'pending' });
$schema->resultset('User')->create({ name => 'Eve', age => 25 });

# --- TESTS ---

sub wait_for {
    my $f = shift;
    my $result;

    # If the future is already done (e.g. returning from cache),
    # just grab the result immediately.
    if ($f->is_ready) {
        return $f->result;
    }

    $f->on_ready(sub {
        my $f = shift;
        $result = eval { $f->result };
        warn "Future failed: $@" if $@;
        $loop->stop;
    });

    $loop->run;
    return $result;
}

subtest "Basic first()" => sub {
    my $rs = $async_schema->resultset('User')->search({}, { order_by => 'name' });
    my $user = wait_for($rs->first);

    isa_ok($user, 'TestSchema::Result::User');
    is($user->name, 'Dave', "First user in alpha order is Dave");
};

subtest "first_future() Alias" => sub {
    my $rs = $async_schema->resultset('User')->search({ name => 'Eve' });
    my $user = wait_for($rs->first_future);

    is($user->name, 'Eve', "first_future works as expected");
};

subtest "first() with Prefetch" => sub {
    my $rs = $async_schema->resultset('User')
        ->search({ 'me.name' => 'Dave' })
        ->prefetch('orders');

    my $user = wait_for($rs->first);

    ok($user, "Found user");
    # Access relationship data directly from the storage slot to ensure its not a lazy-load
    my $orders = $user->{_relationship_data}{orders};
    is(ref $orders, 'ARRAY', "Orders were prefetched into the first() result");
    is($orders->[0]{amount}, 10, "Prefetched data is correct");
};

subtest 'first() from buffered entries' => sub {
    my $rs = $async_schema->resultset('User')->search({}, { order_by => 'name' });

    # Fill the buffer
    wait_for($rs->all);

    # Match the key used in ResultSet.pm line 100
    ok($rs->{_rows}, "Buffer (_rows) is populated");

    my $user = wait_for($rs->first);
    is($user->name, 'Dave', "Retrieved from buffer correctly");
};

done_testing();
