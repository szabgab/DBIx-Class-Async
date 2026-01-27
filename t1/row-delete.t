
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

subtest 'Row Object Delete' => sub {
    my $rs = $async_schema->resultset('User');

    # 1. Setup
    my $user = $rs->create({
        name  => 'Suicidal Row',
        email => 'rip@test.com'
    })->get;

    # CRITICAL CHECK: If this fails, the rest of the subtest is invalid
    my $user_id = $user->id;
    ok($user_id, "Created user with ID: " . ($user_id // 'UNDEFINED'))
        or return; # Exit this subtest early if create is broken

    is($user->in_storage, 1, 'Object starts in_storage => 1');

    # 2. Execution
    my $rows_affected = $user->delete->get;

    # 3. Validation
    is($rows_affected, 1, 'Row->delete reported 1 row affected');
    is($user->in_storage, 0, 'Object now has in_storage => 0');
};

subtest 'Row Object Delete - Not in Storage' => sub {
    # Create a local object that was never saved to DB
    my $new_user = $async_schema->resultset('User')->new_result({ name => 'Ghost' });

    is($new_user->in_storage, 0, 'New result is not in storage');

    my $res = $new_user->delete->get;
    is($res, 0, 'Deleting a non-stored row returns 0 immediately');
};

subtest 'Chained find()->delete()' => sub {
    my $rs = $async_schema->resultset('User');

    # 1. Create the user first
    my $initial_user = $rs->create({ name => 'Chain Test', email => 'chain@test.com' })->get;
    my $target_id = $initial_user->id;

    # 2. The Chain: find -> then -> delete
    my $chain_future = $rs->find($target_id)->then(sub {
        my $user = shift;

        ok($user, "find($target_id) returned a user object");
        isa_ok($user, 'DBIx::Class::Async::Row');

        # Trigger the row-level delete
        return $user->delete;
    });

    # 3. Wait for the whole chain to finish
    my $rows_affected = $chain_future->get;

    is($rows_affected + 0, 1, 'Chain reported 1 row deleted');

    # 4. Final verification
    my $exists = $rs->find($target_id)->get;
    is($exists, undef, 'User is officially gone from the database');
};


done_testing();
