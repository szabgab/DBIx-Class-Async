
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
$setup_schema->resultset('User')->populate([
    [qw/ name age /],
    [ 'Alice', 30 ],
    [ 'Bob',   40 ],
    [ 'Charlie', 20 ],
]);

# 3. Create the Async Schema Object
my $async_schema = DBIx::Class::Async::Schema->connect($dsn, {
    schema_class => 'TestSchema',
    async_loop   => $loop,
    workers      => 2,
});

my $rs = $async_schema->resultset('User');

subtest 'Testing ResultSetColumn Aggregates' => sub {
    my $initial_queries = $rs->stats('queries');

    # 1. Get the column proxy (Synchronous call, should NOT increment counter)
    my $age_col = $rs->get_column('age');
    isa_ok($age_col, 'DBIx::Class::Async::ResultSetColumn', "get_column returned the proxy");
    is($rs->stats('queries'), $initial_queries, "Counter did not increment yet");

    # 2. Test MAX aggregate
    note "Executing MAX(age)...";
    my $max_val;
    my $f_max = $age_col->max;

    use Data::Dumper;
    $f_max->on_done(sub {
        my $val = shift;
        if (ref $val eq 'HASH') {
            note "KEYS IN HASH: " . join(", ", keys %$val);
            note "DUMP: " . Dumper($val);
        }
        $max_val = $val;
        $loop->stop;
    });

    #$f_max->on_done(sub { $max_val = shift; $loop->stop; });
    $loop->run;

    ok(defined $max_val, "Retrieved a maximum age: $max_val");
    is($rs->stats('queries'), $initial_queries + 1, "Counter incremented for MAX query");

    # 3. Test SUM aggregate
    note "Executing SUM(age)...";
    my $sum_val;
    my $f_sum = $age_col->sum;

    $f_sum->on_done(sub { $sum_val = shift; $loop->stop; });
    $loop->run;

    ok(defined $sum_val, "Retrieved a sum of ages: $sum_val");
    is($rs->stats('queries'), $initial_queries + 2, "Counter incremented for SUM query");

    # 4. Test a filtered aggregate (Ensures payload/WHERE clause is preserved)
    # Re-use your search logic
    my $filtered_rs = $rs->search({ name => { '!=', undef } });
    my $f_filtered = $filtered_rs->get_column('age')->min;

    my $min_val;
    $f_filtered->on_done(sub { $min_val = shift; $loop->stop; });
    $loop->run;

    ok(defined $min_val, "Retrieved MIN(age) with filters");
    is($rs->stats('queries'), $initial_queries + 3, "Counter incremented for filtered MIN query");

    done_testing();
};

subtest 'Testing Average Aggregate' => sub {
    my $f_avg = $rs->get_column('age')->average;

    my ($avg_val, $error);

    $f_avg->on_ready(sub {
        my $f = shift;
        if ($f->is_done) {
            $avg_val = $f->result;
        } else {
            $error = $f->failure;
        }
        # ALWAYS stop the loop once the future is ready
        $loop->stop;
    });

    $loop->run;

    if ($error) {
        fail("Average query failed: $error");
    } else {
        # SQLite might return 30 or 30.0
        cmp_ok($avg_val, '==', 30, "Retrieved the correct average age (30)");
    }
    done_testing();
};

subtest 'Testing Column Count' => sub {
    my $f = $rs->get_column('age')->count;
    my $val;

    $f->on_ready(sub {
        my $future = shift;
        if ($future->is_done) {
            $val = $future->result;
        } else {
            # THIS IS THE KEY:
            diag "Worker failed with error: " . $future->failure;
            $val = undef;
        }
        $loop->stop;
    });

    $loop->run;
    is($val, 3, "Column count works");
    done_testing();
};

done_testing();
