

use strict;
use warnings;
use Test::More;
use File::Temp qw(tempfile);
use IO::Async::Loop;
use lib 'lib';
use TestSchema;
use DBIx::Class::Async::Schema;

# 1. Setup real temporary SQLite database
my ($fh, $filename) = tempfile(SUFFIX => '.db', UNLINK => 1);
my $dsn = "dbi:SQLite:dbname=$filename";

# Initialize the real schema to deploy the tables
my $base_schema = TestSchema->connect($dsn);
$base_schema->deploy();

# 2. Initialize the Async Engine
my $loop = IO::Async::Loop->new;
my $async_schema = DBIx::Class::Async::Schema->connect($dsn, {
    schema_class => 'TestSchema',
    async_loop   => $loop,
    workers      => 2,
});

# 3. Get a ResultSet (This will have the _async_db hashref automatically)
my $rs = $async_schema->resultset('User');

# --- Bottom-Up Test of new_result ---

subtest 'Validation of new_result via Async Engine' => sub {
    # Verify the _async_db exists and is a hashref as expected
    ok(ref($rs->{_async_db}) eq 'HASH', 'ResultSet contains the _async_db hashref');

    # Data as it would come from a worker
    my $raw_data = {
        id    => 101,
        name  => 'BottomUp Engine Test',
        email => 'engine@test.com'
    };

    # Call the new_result method we implemented in ResultSet.pm
    my $row = $rs->new_result($raw_data, { in_storage => 1 });

    # A. Check Object Inflation
    ok($row, 'new_result returned an object');
    isa_ok($row, 'DBIx::Class::Async::Row', 'Inherits from Async::Row base');
    isa_ok($row, 'TestSchema::Result::User', 'Inherits from real Result class');

    # B. Check Dynamic Hijacking
    my $class_name = ref($row);
    like($class_name, qr/^DBIx::Class::Async::Anon::/, "Class hijacked to unique namespace: $class_name");

    # C. Verify Data Access
    is($row->id, 101, 'Data integrity: id is correct');
    is($row->name, 'BottomUp Engine Test', 'Data integrity: name is correct');

    # D. Verify Storage State
    ok($row->in_storage, 'Row is correctly marked as in_storage');
};

done_testing();
