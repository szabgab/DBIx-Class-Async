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

# 1. SETUP: Create the Async Schema and Loop
my $loop = IO::Async::Loop->new;
my (undef, $db_file) = tempfile(SUFFIX => '.db', UNLINK => 1);

my $async_schema = DBIx::Class::Async::Schema->connect(
    "dbi:SQLite:dbname=$db_file",
    undef, undef,
    { workers => 1, schema_class => 'TestSchema', async_loop => $loop }
);

# 2. CUSTOM CLASS DEFINITION
# This represents logic the user wants on their result objects
{
    package My::Custom::User;
    use parent 'DBIx::Class::Async::Row';

    sub hello_name {
        my $self = shift;
        # Note: get_column works because DBIx::Class::Async::Row provides it
        return "Hello, " . $self->get_column('name');
    }
}

# 3. SCHEMA METHOD PORT TEST
subtest "Schema class() method" => sub {
    # Testing the ported $schema->class('Source') method
    my $class_name = eval { $async_schema->class('User') };
    diag $@ if $@;

    ok($class_name, "Fetched class name for 'User'");
    is($class_name, 'TestSchema::Result::User', "Default class name matches TestSchema definition");
};

subtest "Result Class Overrides" => sub {
    my $rs = $async_schema->resultset('User');

    # 4. OVERRIDE via search attributes
    my $custom_rs = $rs->search({}, { result_class => 'My::Custom::User' });

    is($custom_rs->result_class, 'My::Custom::User', "result_class accessor returns the override");

    # 5. TEST THE HYBRID BLISSING (new_result logic)
    # This triggers your Lines 47-88 in ResultSet.pm
    my $row = $custom_rs->new_result({ name => 'John', id => 1 });

    # Verify Multiple Inheritance / Anon Class Hijacking
    isa_ok($row, 'My::Custom::User', "Row object 'isa' custom class");
    isa_ok($row, 'DBIx::Class::Async::Row', "Row object 'isa' Async::Row base class");

    # Check if the anon class name followed our rule
    like(ref($row), qr/^DBIx::Class::Async::Anon::/, "Row is blessed into the hybrid anonymous namespace");

    # 6. VERIFY CUSTOM LOGIC
    is($row->hello_name, "Hello, John", "Custom method 'hello_name' executed correctly");
    is($row->get_column('id'), 1, "Standard column data is still accessible");
};

subtest "Chaining and Immutability" => sub {
    my $rs = $async_schema->resultset('User');

    # Check that calling result_class returns a NEW ResultSet (Cloning)
    my $rs_chained = $rs->result_class('My::Custom::User');

    ok($rs_chained != $rs, "ResultSet is cloned, not modified in place");
    is($rs->result_class, 'TestSchema::Result::User', "Original RS remains untouched");
    is($rs_chained->result_class, 'My::Custom::User', "Chained RS has the override");

    # Verify the override persists through a search
    my $rs_next = $rs_chained->search({ active => 1 });
    is($rs_next->result_class, 'My::Custom::User', "result_class persists across chained searches");
};

subtest "Schema class() method Integration" => sub {
    # 1. Direct call to the new method
    my $class_name = $async_schema->class('User');
    is($class_name, 'TestSchema::Result::User', "Direct class() call returns correct string");

    # 2. Indirect call via resultset creation
    my $rs = $async_schema->resultset('User');
    is($rs->result_class, $class_name, "ResultSet correctly inherited class name via Schema::class()");

    # 3. Test error handling
    eval { $async_schema->class('NonExistentSource') };
    like($@, qr/No such source/, "class() croaks correctly on invalid source");
};

done_testing();
