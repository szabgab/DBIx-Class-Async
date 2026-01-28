#!/usr/bin/env perl

use strict;
use warnings;
use Test::More;
use File::Temp qw(tempfile);
use IO::Async::Loop;
use DBIx::Class::ResultSource;
use DBIx::Class::Async::Schema;

my (undef, $db_file) = tempfile(SUFFIX => '.db', UNLINK => 1);
my $loop = IO::Async::Loop->new;
my $async_schema = DBIx::Class::Async::Schema->connect(
    "dbi:SQLite:dbname=$db_file", undef, undef,
    { schema_class => 'TestSchema', async_loop => $loop }
);

subtest "Dynamic Registration Metadata" => sub {
    my $source = DBIx::Class::ResultSource->new({ name => 'temp_table' });
    $source->add_columns( id => { data_type => 'integer' } );
    $source->result_class('TestSchema::Result::User');

    # 1. Register in Parent
    $async_schema->register_source('DynamicSource', $source);

    # 2. Verify Parent can resolve it
    my $rs = eval { $async_schema->resultset('DynamicSource') };
    ok($rs, "Parent created ResultSet for DynamicSource") or diag $@;

    is($async_schema->class('DynamicSource'), 'TestSchema::Result::User',
       "Parent maps DynamicSource to correct Result Class");

    # 3. Handle the Worker limitation
    # We skip the count_future check for now because Workers don't share
    # the Parent's dynamic memory state.
    diag "Note: Workers cannot see DynamicSource unless defined in the schema class file.";
};

done_testing;
