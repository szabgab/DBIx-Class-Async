
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

subtest 'Debug Lookup' => sub {
    my $rs = $async_schema->resultset('User');
    my $data = { 'me.email' => 'test@test.com', name => 'Foo' };

    my $lookup = $rs->_extract_unique_lookup($data, {});

    # If this shows the whole hash instead of just { email => ... },
    # the alias-aware fix above is required.

    ok(exists $lookup->{email}, "Lookup contains the unique column");
    ok(!exists $lookup->{'me.email'}, "Lookup cleaned the alias");
};

subtest 'ResultSet update_or_new logic' => sub {
    my $rs = $async_schema->resultset('User');
    my $email = 'new_test@example.com';

    # 1. Test the "New" path
    my $new_row = $rs->update_or_new({
        'me.email' => $email,
        'name'     => 'Local Ghost'
    })->get;

    isa_ok($new_row, 'DBIx::Class::Async::Row');
    is($new_row->in_storage, 0, 'New row is NOT in storage yet');
    is($new_row->email, $email, 'Key alias "me." was cleaned correctly');
    is($new_row->name, 'Local Ghost', 'Data populated correctly');

    # 2. Test the "Update" path
    # First, save a record to find later
    my $stored = $rs->create({ email => 'stored@example.com', name => 'Stored' })->get;

    my $updated_row = $rs->update_or_new({
        email => 'stored@example.com',
        name  => 'Changed'
    })->get;

    is($updated_row->id, $stored->id, 'Found existing record');
    is($updated_row->name, 'Changed', 'Update triggered on found row');
    is($updated_row->in_storage, 1, 'Updated row is in storage');
};

done_testing();
