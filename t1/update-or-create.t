
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

subtest 'ResultSet update_or_create logic' => sub {
    my $rs = $async_schema->resultset('User');
    my $unique_email = 'uoc_test@example.com';

    # 1. Test the "Create" path
    my $created = $rs->update_or_create({
        email => $unique_email,
        name  => 'Initial'
    })->get;

    ok($created->id, 'Created new row with ID');
    is($created->in_storage, 1, 'Row is in storage');

    # 2. Test the "Update" path
    my $updated = $rs->update_or_create({
        email => $unique_email,
        name  => 'Revised'
    })->get;

    is($updated->id, $created->id, 'Identified and updated the same row');
    is($updated->name, 'Revised', 'Data updated correctly');

    # 3. Test Conflict/Race Condition (Simulation)
    # We pass a duplicate email but pretend we didn't see it in 'find'.
    # This triggers the 'catch' block in your implementation.
    my $conflict_future = $rs->create({ email => 'conflict@test.com', name => 'First' })
        ->then(sub {
            # Try to create it again - this SHOULD trigger the race recovery catch
            return $rs->update_or_create({ email => 'conflict@test.com', name => 'Second' });
        });

    my $recovered = $conflict_future->get;
    is($recovered->name, 'Second', 'Race recovery successful: caught conflict and updated');
};

done_testing();
