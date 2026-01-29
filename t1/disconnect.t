#!/usr/bin/env perl

use strict;
use warnings;
use Test::More;
use IO::Async::Loop;
use File::Temp qw(tempfile);
use DBIx::Class::Async::Schema;

use lib 't/lib';
use TestSchema;

BEGIN {
    $SIG{__WARN__} = sub {};
}

my $loop = IO::Async::Loop->new;
my (undef, $db_file) = tempfile(SUFFIX => '.db', UNLINK => 1);

my $async_schema = DBIx::Class::Async::Schema->connect(
    "dbi:SQLite:dbname=$db_file",
    undef, undef,
    { workers => 1, schema_class => 'TestSchema', async_loop => $loop }
);

subtest "Verify Active Connection" => sub {
    ok($async_schema->{_async_db}, "Internal async_db manager is initialized");

    # Perform a quick operation to ensure workers are alive
    my $rs = $async_schema->resultset('User');
    isa_ok($rs, 'DBIx::Class::Async::ResultSet');

    # Ensure metadata cache is populated
    my $class = $async_schema->class('User');
    ok($class, "Metadata cache is populated (Class: $class)");
    ok(keys %{$async_schema->{_sources_cache}}, "Sources cache is not empty");
};

subtest "Execution of disconnect()" => sub {
    # We call the method we just ported
    eval { $async_schema->disconnect };
    ok(!$@, "disconnect() executed without errors") or diag $@;

    # 1. Check reference cleanup
    ok(!exists $async_schema->{_async_db}, "Internal _async_db reference was deleted");

    # 2. Check metadata cleanup
    # (Assuming your disconnect clears the cache as suggested)
    is(scalar keys %{$async_schema->{_sources_cache} // {}}, 0, "Metadata sources cache was cleared");
};

subtest "Post-disconnect Behavior" => sub {
    # Attempting to use the schema after disconnect should fail gracefully
    # because the worker pool is gone.
    eval { $async_schema->resultset('User')->all_future->get };

    ok($@, "Operations fail after disconnect (as expected)");
    like($@, qr/async_db|disconnected|undef|Schema class not found/i,
         "Error message correctly identifies missing connection");
};

done_testing;
