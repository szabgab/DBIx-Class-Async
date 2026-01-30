#!/usr/bin/env perl

use strict;
use warnings;

use Test::More;
use File::Temp;
use IO::Async::Loop;
use DBIx::Class::Async::Schema;

use lib 't/lib';

my $loop           = IO::Async::Loop->new;
my ($fh, $db_file) = File::Temp::tempfile(SUFFIX => '.db', UNLINK => 1);
my $schema         = DBIx::Class::Async::Schema->connect(
    "dbi:SQLite:dbname=$db_file", undef, undef, {},
    { workers      => 2,
      schema_class => 'TestSchema',
      async_loop   => $loop,
      cache_ttl    => 60,
    },
);

$schema->await($schema->deploy({ add_drop_table => 1 }));

my $alice = $schema->resultset('User')
                   ->create({
                        name  => 'Alice',
                        email => 'alice@example.com', })
                   ->get;
my $john  = $schema->resultset('User')
                   ->create({
                        name  => 'John',
                        email => 'john@example.com', })
                   ->get;

my $users = $schema->resultset('User')
                   ->search
                   ->all
                   ->get;

my $count = $schema->resultset('User')
                   ->count
                   ->get;

is($schema->total_queries, 5, 'total queries');
is($schema->error_count,   0, 'error count');
is($schema->cache_misses,  1, 'cache misses');
is($schema->cache_retries, 0, 'cache retries');

$schema->disconnect;

done_testing;
