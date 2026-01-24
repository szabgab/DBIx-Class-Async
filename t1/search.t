use strict;
use warnings;
use Test::More;
use IO::Async::Loop;
use File::Temp qw(tempfile);
use TestSchema;
use DBIx::Class::Async::Schema;

BEGIN {
    $SIG{__WARN__} = sub {};
}

# 1. Setup the physical environment
my $loop = IO::Async::Loop->new;
my ($fh, $db_file) = tempfile(SUFFIX => '.db', UNLINK => 1);
close $fh;
my $dsn = "dbi:SQLite:dbname=$db_file";

note "Database file: $db_file";

# 2. Deploy the database (Synchronously, just for setup)
my $setup_schema = TestSchema->connect($dsn);

# 1. Setup sample data
$setup_schema->deploy;
$setup_schema->resultset('User')->create({
    name => 'Alice',
    email => 'alice@test.com',
    active => 1
});
$setup_schema->resultset('User')->create({
    name => 'Bob',
    email => 'bob@test.com',
    active => 1
});

my $async_schema = DBIx::Class::Async::Schema->connect($dsn, {
    schema_class => 'TestSchema',
    loop         => $loop,  # ← Use 'loop' not 'async_loop'
    workers      => 2,
});

my $rs = $async_schema->resultset('User')
                ->search({ active => 1 })
                ->search({ name => 'Alice' });

is(ref($rs), 'DBIx::Class::Async::ResultSet', 'Still an Async ResultSet after chaining');
is_deeply($rs->{_cond}, { -and => [ { active => 1 }, { name => 'Alice' } ] }, 'Conditions merged correctly');

# 3. Test Execution via Worker
my $future = $rs->all();

# Wait for the worker to finish
$loop->await($future);
my $results = $future->get;

is(ref($results), 'ARRAY', 'Worker returned an arrayref');
is(scalar @$results, 1, 'Correctly filtered to 1 result');
is($results->[0]{name}, 'Alice', 'Data is correct');
is($results->[0]{email}, 'alice@test.com', 'Email matches');

done_testing();
