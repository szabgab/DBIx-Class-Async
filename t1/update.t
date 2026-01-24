
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

subtest 'ResultSet Update - Path A (Fast Path)' => sub {
    my $rs = $async_schema->resultset('User')->search({ id => 1 });

    # 1. Dispatch the update
    # This should trigger Path A because there are no attrs
    my $future = $rs->update({ name => 'Updated Name' });

    isa_ok($future, 'Future', 'update() returns a Future');
    my $rows_affected = $future->get;

    is($rows_affected, 1, 'Successfully updated 1 row');

    # 2. Verify the update by fetching a fresh ResultSet
    # (Don't use the old $rs because it might have the old name cached in _rows!)
    my $fresh_rs = $async_schema->resultset('User')->search({ id => 1 });
    my $user = $fresh_rs->next->get;

    is($user->name, 'Updated Name', 'Database reflects the update');
};

subtest 'ResultSet Update - Path B (Safe Path)' => sub {
    # Adding 'rows => 1' forces Path B
    my $rs = $async_schema->resultset('User')->search(
        { id => 1 },
        { rows => 1 }
    );

    my $future = $rs->update({ name => 'Path B Winner' });

    # Check logs for "Taking Path B" and "update_all"
    my $rows_affected = $future->get;
    is($rows_affected, 1, 'Safe Path updated correct number of rows');

    # Verify
    my $user = $async_schema->resultset('User')->find(1)->get;
    is($user->name, 'Path B Winner', 'Data updated via Safe Path');
};

done_testing();
