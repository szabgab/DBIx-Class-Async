
use strict;
use warnings;
use Test::More;
use IO::Async::Loop;
use File::Temp qw(tempfile);

# The pieces we've built
use TestSchema;
use DBIx::Class::Async::Schema;

BEGIN {
    $SIG{__WARN__} = sub {};
}

# 1. Setup the physical environment
my $loop = IO::Async::Loop->new;
my ($fh, $db_file) = tempfile(SUFFIX => '.db', UNLINK => 1);
my $dsn = "dbi:SQLite:dbname=$db_file";

# 2. Deploy the database
my $setup_schema = TestSchema->connect($dsn);
$setup_schema->deploy();
$setup_schema->resultset('User')->create({ name => 'Alice', active => 1 });
$setup_schema->resultset('User')->create({ name => 'Bob',   active => 1 });
$setup_schema->resultset('User')->create({ name => 'Charlie', active => 0 });

# 3. Create the Async Schema Object
my $async_schema = DBIx::Class::Async::Schema->connect($dsn, {
    schema_class => 'TestSchema',
    async_loop   => $loop,
    workers      => 2,
});

my $rs = $async_schema->resultset('User');

# --- TEST 1: Standard Count ---
{
    note "Testing standard async count...";
    my $future = $rs->count({ active => 1 });

    my $count;
    $future->on_done(sub { $count = shift; $loop->stop; });
    $loop->run;

    is($count, 2, "Standard count returned 2 active users");
}

# --- TEST 2: Count Literal (The New Design Port) ---
{
    note "Testing async count_literal...";
    # Literal SQL fragment for SQLite
    my $future = $rs->count_literal('name LIKE ?', 'A%');

    my $literal_count;
    $future->on_done(sub { $literal_count = shift; $loop->stop; });
    $loop->run;

    is($literal_count, 1, "count_literal found 1 user starting with 'A' (Alice)");
}

# --- TEST 3: Scoreboard Integrity ---
{

    # Use the new public method
    my $queries = $rs->stats('queries');

    # If you didn't reset, we expect 4 (2 setup + 2 test)
    is($queries, 2, "The public stats() method correctly reported 4 queries");
}

subtest 'Testing count_rs and lazy stats' => sub {
    # 1. Get current query count
    my $initial_queries = $rs->stats('queries');

    # 2. Call count_rs (Lazy)
    # This should return a new ResultSet object immediately
    my $count_rs = $rs->count_rs({ active => 1 });

    isa_ok($count_rs, 'DBIx::Class::Async::ResultSet', "count_rs returned a ResultSet");
    is($rs->stats('queries'), $initial_queries, "Counter NOT incremented after count_rs (Lazy)");

    # 3. Execute the RS using the async count method
    note "Executing count from the lazy ResultSet...";
    my $future = $count_rs->count;

    my $val;
    $future->on_done(sub {
        $val = shift;
        $loop->stop;
    });
    $loop->run;

    # 4. Verify results
    is($val, 2, "Execution of count_rs returned correct data");
    is($rs->stats('queries'), $initial_queries + 1, "Counter incremented exactly once after execution");

    done_testing();
};

subtest 'Testing error metrics' => sub {
    # 1. Capture current error count
    my $initial_errors = $rs->stats('errors') || 0;

    # Trigger a guaranteed SQL syntax error
    my $bad_rs = $rs->search_literal('THIS IS NOT VALID SQL');

    my $future = $bad_rs->count;

    # Use on_ready to ensure the loop ALWAYS stops
    $future->on_ready(sub { $loop->stop });

    $loop->run;

    is($rs->stats('errors'), $initial_errors + 1, "The Accountant caught the syntax error");
    done_testing();
};

done_testing();
