
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


subtest 'ResultComponent: Update' => sub {
    # 1. Fetch a user via the async engine
    my $user_future = $async_schema->resultset('User')->find(1);
    my $user = $user_future->get;

    ok($user, 'Fetched user for update test');
    isa_ok($user, 'DBIx::Class::Async::Row');

    # 2. Modify name and trigger update_future
    $user->name('Updated Via Component');
    my $update_f = $user->update_future();

    isa_ok($update_f, 'Future', 'update_future returns a Future');
    my $updated_row = $update_f->get;

    # 3. Verify local object and database state
    is($updated_row->name, 'Updated Via Component', 'Local row object name updated');

    # Verify in DB via base schema
    my $db_user = $base_schema->resultset('User')->find(1);
    is($db_user->name, 'Updated Via Component', 'Database actually updated');
};

subtest 'ResultComponent: Delete' => sub {
    # 1. Fetch the user again (Use 'find' instead of 'find_future')
    my $user = $async_schema->resultset('User')->find(1)->get;

    # 2. Trigger delete_future
    my $delete_f = $user->delete_future();
    isa_ok($delete_f, 'Future', 'delete_future returns a Future');

    $delete_f->get;

    # 3. Verify deletion in database via the base (synchronous) schema
    my $db_user = $base_schema->resultset('User')->find(1);
    ok(!$db_user, 'User successfully deleted from database via component');
};

subtest 'ResultComponent: No-op Update' => sub {
    # Re-create user for no-op test
    $base_schema->resultset('User')->create({ id => 2, name => 'NoOp', email => 'noop@test.com' });

    # FIX: Change find_future to find
    my $user = $async_schema->resultset('User')->find(2)->get;

    # Call update without changes
    my $f = $user->update_future();
    my $res = $f->get;

    is($res, $user, 'Update without changes returns the object immediately');
};

done_testing();
