
use strict;
use warnings;
use Test::More;
use IO::Async::Loop;
use File::Temp qw(tempfile);

use Data::Dumper;

# The pieces we've built
use TestSchema;
use DBIx::Class::Async::Schema;

# 1. Setup the physical environment
my $loop = IO::Async::Loop->new;
my ($fh, $db_file) = tempfile(SUFFIX => '.db', UNLINK => 1);
my $dsn = "dbi:SQLite:dbname=$db_file";

# 2. Deploy the database (Synchronously, just for setup)
my $setup_schema = TestSchema->connect($dsn);
$setup_schema->deploy();
$setup_schema->resultset('User')->create({ name => 'Alice', active => 1 });
$setup_schema->resultset('User')->create({ name => 'Bob',   active => 1 });

# --- THE ASYNC START ---

# 3. Create the Async Schema Object
# This is the "Engine" being built and workers being forked
my $async_schema = DBIx::Class::Async::Schema->connect($dsn, {
    schema_class => 'TestSchema',
    async_loop   => $loop,
    workers      => 2,
});

# 4. Get the Async ResultSet
# This uses our overridden resultset() method to return a DBIx::Class::Async::ResultSet
my $rs = $async_schema->resultset('User');

# 5. Execute the First Count
note "Dispatching async count request...";
my $future = $rs->count({ active => 1 });

# ← CHANGE: Use on_ready callback and run the loop
my $count;
$future->on_ready(sub {
    my $f = shift;
    $count = $f->get;
    $loop->stop;
});

$loop->run;  # ← Run the loop until future completes

is($count, 2, "The async count returned the correct number of rows");

done_testing();
