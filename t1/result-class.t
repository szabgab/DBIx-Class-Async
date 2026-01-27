use strict;
use warnings;
use Test::More;
use File::Temp qw(tempfile);
use lib 'lib', 't/lib';

BEGIN {
    $SIG{__WARN__} = sub {};
}

# 1. Setup Database
my ($fh, $filename) = tempfile(SUFFIX => '.db', UNLINK => 1);
my $dsn = "dbi:SQLite:dbname=$filename";

use TestSchema;
my $native_schema = TestSchema->connect($dsn);
$native_schema->deploy();

use DBIx::Class::Async::Schema;
my $async_schema = DBIx::Class::Async::Schema->connect(
    $dsn, { schema_class => 'TestSchema', workers => 1 }
);

## Subtest: Result Class Resolution
subtest 'Result class detection and override' => sub {
    my $rs = $async_schema->resultset('User');

    # 1. Default Behavior
    is(
        $rs->result_class,
        'TestSchema::Result::User',
        "Correctly resolved default result_class from Source"
    );

    # 2. Manual Override (Standard Object)
    $rs->result_class('TestSchema::Result::Order');
    is(
        $rs->result_class,
        'TestSchema::Result::Order',
        "Override correctly updated _attrs"
    );

    # 3. HashRefInflator Override
    # This is a common pattern for high-performance reads
    my $hash_inflator = 'DBIx::Class::ResultClass::HashRefInflator';
    $rs->result_class($hash_inflator);
    is($rs->result_class, $hash_inflator, "Can set HashRefInflator");
};

done_testing();
