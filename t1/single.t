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
    return $f->result if $f->is_ready; # Handle immediate cache hits
    $f->on_ready(sub {
        my $f = shift;
        $res = eval { $f->result };
        $loop->stop;
    });
    $loop->run;
    return $res;
}

# Seed Data
my $schema = TestSchema->connect($dsn);
$schema->resultset('User')->create({ name => 'Alice', age => 30 });
my $bob = $schema->resultset('User')->create({ name => 'Bob', age => 40 });
$bob->create_related('orders', { amount => 99.99, status => 'shipped' });

# --- TESTS ---

subtest 'Basic single()' => sub {
    # single() usually implies we expect exactly one result
    my $rs = $async_schema->resultset('User')->search({ name => 'Bob' });
    my $user = wait_for($rs->single);

    isa_ok($user, 'TestSchema::Result::User');
    is($user->name, 'Bob', "single() found the correct user");
};

subtest 'single_future() with Prefetch' => sub {
    my $rs = $async_schema->resultset('User')
        ->search({ 'me.name' => 'Bob' })
        ->prefetch('orders');

    my $user = wait_for($rs->single_future);

    ok($user, "Found user via single_future");
    is($user->name, 'Bob', "Correct user returned");

    # Verify prefetch worked through the single() path
    my $orders = $user->{_relationship_data}{orders};
    is(ref $orders, 'ARRAY', "Orders were prefetched during single() call");
    is($orders->[0]{amount}, 99.99, "Related data is intact");
};

subtest 'single() returns undef on no match' => sub {
    my $rs = $async_schema->resultset('User')->search({ name => 'NonExistent' });
    my $user = wait_for($rs->single);

    is($user, undef, "single() returns undef when no rows match");
};

subtest 'single() utilizes existing buffer' => sub {
    my $rs = $async_schema->resultset('User')->search({});

    # Load all users into memory first
    wait_for($rs->all);
    ok($rs->{_rows}, "Buffer is populated");

    # Now call single() - it should return the first item without a new DB hit
    # (If you have STAGE logs enabled, you'll see no 'all' sent to worker here)
    my $user = wait_for($rs->single);
    is($user->name, 'Alice', "single() pulled from buffer correctly");
};

done_testing();
