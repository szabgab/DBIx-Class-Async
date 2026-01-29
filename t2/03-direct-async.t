#!/usr/bin/env perl

use strict;
use warnings;
use utf8;

use Test::More;
use Test::Exception;

use FindBin qw($Bin);
use lib "$Bin/../lib";
use lib "$Bin/lib";

use DBI;
use TestSchema;
use IO::Async::Loop;
use DBIx::Class::Async::Schema;

use File::Temp;
my ($fh, $db_file) = File::Temp::tempfile(SUFFIX => '.db', UNLINK => 1);

my $schema_setup = TestSchema->connect("dbi:SQLite:dbname=$db_file");
$schema_setup->deploy({ add_drop_table => 1 });

my $loop = IO::Async::Loop->new;

# Test 1: Schema-based connection (Replaces Direct Async connection)
subtest 'Schema-based async connection' => sub {

    my $schema;
    lives_ok {
        $schema = DBIx::Class::Async::Schema->connect(
            "dbi:SQLite:dbname=$db_file",
            undef,
            undef,
            {},
            {
                workers      => 2,
                schema_class => 'TestSchema',
                async_loop   => $loop
            }
        );
    } 'Schema connects successfully';

    isa_ok($schema, 'DBIx::Class::Async::Schema');

    # Mapping 'search' to ResultSet 'search' (returning future)
    my $search_future = $schema->resultset('User')->search({ balance => { '>' => 100 } })->all;
    isa_ok($search_future, 'IO::Async::Future', 'all() returns a Future');

    my $users = $search_future->get;
    isa_ok($users, 'ARRAY');
    is(scalar @$users, 0, 'No users initially');
};

# Test 2: Create, Find, Update, and Count via ResultSet
subtest 'ResultSet operations' => sub {
    # Ensure a fresh loop and schema for this subtest
    my $schema = DBIx::Class::Async::Schema->connect(
        "dbi:SQLite:dbname=$db_file",
        undef, undef, {},
        { workers => 2, schema_class => 'TestSchema', async_loop => $loop }
    );

    # Use eval/catch logic for the 'get' calls to see meaningful errors
    # instead of just crashing with 255.

    # 1. Create user
    my $user;
    eval {
        $user = $schema->resultset('User')->create({
            name   => 'Direct Test',
            email  => 'direct@example.com',
            active => 1,
            # balance => 100, # Uncomment this once the column exists in Schema!
        })->get;
    };
    if ($@) {
        fail("Create failed: $@");
        return; # Skip rest of subtest if we can't create
    }

    isa_ok($user, 'HASH', 'create returns a deflated hashref');
    ok(defined $user->id, "User has an ID after create: " . $user->id);
    is($user->name, 'Direct Test', 'Name matches');
    is($user->email, 'direct@example.com', 'Email matches');

    # 2. Find user
    my $found;
    eval {
        $found = $schema->resultset('User')->find($user->id)->get;
    };

    if (isa_ok($found, 'HASH', 'find returns hashref')) {
        is($found->{name}, 'Direct Test', 'Found the correct user');
    }

    # 3. Update user
    # Note: We pass the identity in the first hash, new values in the second
    eval {
        $schema->resultset('User')->update(
            { id => $user->id },
            { name => 'Updated Direct', active => 0 }
        )->get;
    };
    ok(!$@, 'Update operation executed without exception') or diag("Update error: $@");

    # 4. Verification: find again to confirm persistence
    my $refetched = eval { $schema->resultset('User')->find($user->id)->get };
    if ($refetched) {
        is($refetched->{name}, 'Updated Direct', 'Name update persisted');
        is($refetched->{active}, 0, 'Active status update persisted');
    }

    # 5. Count users
    my $count = eval { $schema->resultset('User')->count({})->get };
    cmp_ok($count, '>=', 1, "Count returned: $count");

    # 6. Execute search and get the ARRAY of objects
    my $inactive_users = $schema->resultset('User')->search({ active => 0 })->all->get;

    # 7. Safety check: make sure we actually got an array with an element
    ok(defined $inactive_users && scalar @$inactive_users > 0, "Search returned results");

    # 8. Access using method syntax
    if ($inactive_users && $inactive_users->[0]) {
        is($inactive_users->[0]->name, 'Updated Direct', 'Search found the inactive user');
    }

    done_testing();
};

done_testing();
