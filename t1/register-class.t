#!/usr/bin/env perl

use strict;
use warnings;
use Test::More;
use IO::Async::Loop;
use DBIx::Class::Async::Schema;

# 1. Define a Mock Result Class manually
{
    package My::Manual::User;
    use base 'DBIx::Class::Core';
    __PACKAGE__->table('manual_users');
    __PACKAGE__->add_columns(
        id   => { data_type => 'integer', is_auto_increment => 1 },
        name => { data_type => 'varchar', size => 100 },
    );
    __PACKAGE__->set_primary_key('id');
}

# 2. Setup Schema
my $loop = IO::Async::Loop->new;
my $async_schema = DBIx::Class::Async::Schema->connect(
    "dbi:SQLite::memory:", undef, undef,
    { schema_class => 'TestSchema', async_loop => $loop }
);

subtest "Manual Class Registration" => sub {
    # 3. Test the ported register_class method
    # This should load the class and call register_source internally
    eval { $async_schema->register_class('ManualUser', 'My::Manual::User') };
    ok(!$@, "register_class executed without error") or diag $@;

    # 4. Verify Metadata in Parent
    is($async_schema->class('ManualUser'), 'My::Manual::User',
       "Schema correctly mapped 'ManualUser' to the class string");

    # 5. Verify ResultSet Creation
    my $rs = eval { $async_schema->resultset('ManualUser') };
    ok($rs, "Created ResultSet for the manually registered class");
    isa_ok($rs, 'DBIx::Class::Async::ResultSet');

    # 6. Verify Source Attributes
    my $source = $rs->result_source;
    is($source->from, 'manual_users', "ResultSource correctly identifies table name");
    ok($source->has_column('name'), "ResultSource has the correct columns");
};

done_testing();
