#!/usr/bin/env perl

use strict;
use warnings;
use Test::More;
use IO::Async::Loop;
use File::Temp qw(tempfile);
use DBIx::Class::Async::Schema;

BEGIN {
    $SIG{__WARN__} = sub {};
}

my $loop = IO::Async::Loop->new;
my (undef, $db_file) = tempfile(SUFFIX => '.db', UNLINK => 1);

# Workers => 1 is mandatory for manual txn_begin/commit/rollback
# to ensure all commands stay on the same database handle.
my $async_schema = DBIx::Class::Async::Schema->connect(
    "dbi:SQLite:dbname=$db_file", undef, undef,
    { workers => 1, schema_class => 'TestSchema', async_loop => $loop }
);

$async_schema->deploy->get;

subtest "Manual Transaction: Rollback" => sub {
    # 1. Start Transaction
    $async_schema->txn_begin->get;

    # 2. Create a user
    $async_schema->resultset('User')->create({
        name => 'Ghost',
        email => 'ghost@test.com'
    })->get;

    # 3. Roll it back
    $async_schema->txn_rollback->get;

    # 4. Verify the database is empty
    my $count = $async_schema->resultset('User')->count->get;
    is($count, 0, "Rollback successful: User was not saved");

    done_testing;
};

subtest "Manual Transaction: Commit" => sub {
    $async_schema->txn_begin->get;

    $async_schema->resultset('User')->create({
        name => 'Permanent',
        email => 'perm@test.com'
    })->get;

    $async_schema->txn_commit->get;

    my $count = $async_schema->resultset('User')->count->get;
    is($count, 1, "Commit successful: User was saved");

    done_testing;
};

done_testing;
