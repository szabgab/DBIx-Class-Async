#!/usr/bin/env perl

use strict;
use warnings;
use Test::More;
use IO::Async::Loop;
use File::Temp qw(tempfile);
use DBIx::Class::Async::Schema;

my $loop = IO::Async::Loop->new;
my (undef, $db_file) = tempfile(SUFFIX => '.db', UNLINK => 1);

my $async_schema = DBIx::Class::Async::Schema->connect(
    "dbi:SQLite:dbname=$db_file", undef, undef,
    { schema_class => 'TestSchema', async_loop => $loop }
);

subtest "Unregistration and Cleanup" => sub {
    # 1. Verify existence (assuming 'User' is in TestSchema)
    ok($async_schema->source('User'), "User source initially exists");

    # 2. Unregister
    $async_schema->unregister_source('User');

    # 3. Verify it's gone from the Parent's perspective
    my $rs = eval { $async_schema->resultset('User') };
    ok(!$rs, "resultset('User') fails after unregistration");
    like($@, qr/Can't find source/, "Error message confirms source is gone");
};

done_testing;
